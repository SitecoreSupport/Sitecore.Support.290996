using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Sitecore.Framework.Conditions;
using Sitecore.Framework.Publishing.Data.AdoNet;
using Sitecore.Framework.Publishing.Data.Classic;
using Sitecore.Framework.Publishing;

using SourceSchema = Sitecore.Framework.Publishing.Data.Source.Sql.Schema;
using TargetSchema = Sitecore.Framework.Publishing.Data.Target.Sql.Schema;
using LinksSchema = Sitecore.Framework.Publishing.Data.Links.Sql.Schema;
using Sitecore.Framework.Publishing.Item;

namespace Sitecore.Support.Framework.Publishing.Data.Classic.SqlServer
{
  public class ClassicSqlServerDataProvider: Sitecore.Framework.Publishing.Data.Classic.IClassicItemDataProvider<IDatabaseConnection>
  {
    private readonly ILogger<ClassicSqlServerDataProvider> _logger;

    public ClassicSqlServerDataProvider(ILogger<ClassicSqlServerDataProvider> logger)
    {
      Condition.Requires(logger, nameof(logger)).IsNotNull();

      _logger = logger;
    }

    public object SqlServerUtils { get; private set; }

    public async new Task<Tuple<ItemDataChangeEntity[], FieldDataChangeEntity[]>> AddOrUpdateVariants(
    IDatabaseConnection connection,
    IReadOnlyCollection<ItemDataEntity> itemDatas,
    IReadOnlyCollection<FieldDataEntity> itemFields,
    IReadOnlyCollection<Guid> invariantFieldsToReport,
    IReadOnlyCollection<Guid> langVariantFieldsToReport,
    IReadOnlyCollection<Guid> variantFieldsToReport,
    bool calculateChanges = true)
    {
      var itemTable = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildItemDataTable(itemDatas);
      var fieldsTable = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildFieldDataTable(itemFields);

      var invariantsToReport = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(invariantFieldsToReport);
      var langVariantsToReport = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(langVariantFieldsToReport);
      var variantsToReport = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(variantFieldsToReport);

      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
                    SupportSetItemVariants,
                    new
                    {
                      Items = itemTable,
                      Fields = fieldsTable,
                      UriMarkerFieldId = PublishingConstants.StandardFields.Revision,
                      CalculateChanges = calculateChanges ? 1 : 0,
                      InvariantFieldsToReport = invariantsToReport,
                      LangVariantFieldsToReport = langVariantsToReport,
                      VariantFieldsToReport = variantsToReport
                    },
                    transaction: connection.Transaction,
                    commandType: CommandType.Text,
                    commandTimeout: connection.CommandTimeout).ConfigureAwait(false))
        {
          var report = new Tuple<ItemDataChangeEntity[], FieldDataChangeEntity[]>(
                  new ItemDataChangeEntity[0],
                  new FieldDataChangeEntity[0]);

          if (calculateChanges)
          {
            report = await GetChangeReport(results, invariantFieldsToReport, langVariantFieldsToReport, variantFieldsToReport).ConfigureAwait(false);
          }

          // Because there is a mixture of DML and read queries, this is required in order to force
          // processing of all previous statements. (PBI - 21729)
          var number = await results.ReadAsync<int>().ConfigureAwait(false);

          return report;
        }
      }).ConfigureAwait(false);
    }

    protected virtual string SupportSetItemVariants
    {
      get {
        return @"-- [Publishing_Data_Set_ItemVariants]
	SET NOCOUNT ON;

	BEGIN TRY

		DECLARE @Uris TABLE(
			Id UNIQUEIDENTIFIER, 
			Language NVARCHAR(50), 
			Version INT, 
			UNIQUE NONCLUSTERED (Id, Language, Version));

		INSERT INTO @Uris
		SELECT ItemId, Language, Version
		FROM @Fields revisionField
		WHERE revisionField.FieldId = @UriMarkerFieldId

		-- report changes
		IF @CalculateChanges = 1
		BEGIN
			SELECT newItem.Id,
				   CASE WHEN dbItem.Id IS NULL 
					THEN 'Created' 
					ELSE 'Updated' END AS EditType,

				   CASE WHEN dbItem.Name <> newItem.Name 
					THEN dbItem.Name 
					ELSE NULL END	   AS OriginalName,

				   newItem.Name		   AS NewName,

				   CASE WHEN dbItem.TemplateId <> newItem.TemplateId 
					THEN dbItem.TemplateId 
					ELSE NULL END	   AS OriginalTemplateId,

				   newItem.TemplateId  AS NewTemplateId,
				   CASE WHEN dbItem.MasterId <> newItem.MasterId 
					THEN dbItem.MasterId 
					ELSE NULL END	   AS OriginalMasterId,

				   newItem.MasterId    AS NewMasterId,

				   CASE WHEN dbItem.ParentId <> newItem.ParentId 
					THEN dbItem.ParentId 
					ELSE NULL END	   AS OriginalParentId,

				   newItem.ParentId    AS NewParentId

			FROM		@Items newItem
				LEFT JOIN	[Items] dbItem
					ON			dbItem.[Id] = newItem.[Id]
		END

		-- Update existing
		UPDATE		[Items]
		SET			Name       = newItem.Name,
					TemplateID = newItem.TemplateId,
					MasterID   = newItem.MasterId,
					ParentID   = newItem.ParentId,
					Updated    = newItem.Updated
		FROM		@Items newItem
			INNER JOIN	[Items] dbItem
			ON			dbItem.[Id] = newItem.[Id]

		WHERE dbItem.Name		 <> newItem.Name OR
			  dbItem.TemplateID  <> newItem.TemplateId OR
			  dbItem.ParentID	 <> newItem.ParentId OR
			  dbItem.MasterID	 <> newItem.MasterId
			  
		-- Insert new
		INSERT INTO	[Items] (ID, Name, TemplateID, MasterID, ParentID, Created, Updated)
		SELECT		newItem.[Id], 
					newItem.[Name], 
					newItem.[TemplateId], 
					newItem.[MasterId], 
					newItem.[ParentId], 
					newItem.[Created], 
					newItem.[Updated]
		FROM		@Items newItem
			LEFT JOIN	[Items] dbItem
				ON		dbItem.[Id] = newItem.[Id]
		WHERE		dbItem.[Id] IS NULL

		------------ Shared Fields -------------------------

		-- report changes
		IF @CalculateChanges = 1 AND EXISTS (Select 1 from @InvariantFieldsToReport)
		BEGIN
			SELECT newField.ItemId, 
				   newField.FieldId,
				   newField.Language, 
				   newField.Version, 
				   newField.Value,
				   dbField.Value AS OriginalValue,
				   CASE 
						WHEN dbField.Id IS NULL THEN 'Created'
						WHEN dbField.Value = newField.Value COLLATE Latin1_General_CS_AS THEN 'Unchanged' -- Sitecore.Support.290996
						ELSE 'Updated' 
				   END AS EditType

			FROM @Fields newField

				INNER JOIN @InvariantFieldsToReport reportField
					on reportField.Id = newField.FieldId

				LEFT OUTER JOIN SharedFields dbField
					ON dbField.ItemId   = newField.ItemId   AND
					   dbField.FieldId  = newField.FieldId  AND
					   newField.Language IS NULL				AND
					   newField.Version  IS NULL
		END

		-- Update existing
		UPDATE		[SharedFields]
		SET			Value   = newField.Value,
					Updated = newField.Updated

		FROM		@Fields newField
			INNER JOIN	[SharedFields] dbField
				ON	dbField.[ItemId]  = newField.[ItemId] AND
					dbField.[FieldId] = newField.[FieldId]
		
		WHERE dbField.Value <> newField.Value COLLATE Latin1_General_CS_AS -- Sitecore.Support.290996

		-- Insert new
		INSERT INTO	[SharedFields] ([Id], [ItemId], [FieldId], [Value], [Created], [Updated])
		SELECT		newField.[Id], 
					newField.[ItemId], 
					newField.[FieldId], 
					newField.[Value], 
					newField.[Created], 
					newField.[Updated]

		FROM		@Fields newField
			LEFT JOIN	[SharedFields] dbField
				ON	dbField.[ItemId]  = newField.[ItemId] AND			
					dbField.[FieldId] = newField.[FieldId]

		WHERE		newField.[Language] IS NULL AND
					newField.[Version] IS NULL AND
					dbField.[Id] IS NULL

		-- Delete removed fields
		DELETE FROM [SharedFields]
		FROM		[SharedFields] dbField
			
			INNER JOIN (
					SELECT DISTINCT Id
					FROM @Uris uri) uniqueId
				ON uniqueId.Id = dbField.ItemId

			LEFT JOIN @Fields newField
				ON dbField.ItemId = newField.ItemId AND
				   dbField.FieldId = newField.FieldId AND
				   newField.Language IS NULL AND
				   newField.Version IS NULL
		WHERE newField.ItemId IS NULL

		------------ Unversioned Fields -------------------------

		-- report changes
		IF @CalculateChanges = 1 AND EXISTS (Select 1 from @LangVariantFieldsToReport)
		BEGIN
			SELECT newField.ItemId, 
				   newField.FieldId,
				   newField.Language, 
				   newField.Version, 
				   newField.Value,
				   dbField.Value AS OriginalValue,
				   CASE 
						WHEN dbField.Id IS NULL THEN 'Created'
						WHEN dbField.Value = newField.Value COLLATE Latin1_General_CS_AS THEN 'Unchanged' -- Sitecore.Support.290996
						ELSE 'Updated' 
				   END AS EditType
			FROM @Fields newField

				INNER JOIN @LangVariantFieldsToReport reportField
					on reportField.Id = newField.FieldId

				LEFT OUTER JOIN UnversionedFields dbField
					ON dbField.ItemId   = newField.ItemId   AND
					   dbField.FieldId  = newField.FieldId  AND
					   dbField.Language = newField.Language AND
					   newField.Version IS NULL
		END

		-- Update existing
		UPDATE		[UnversionedFields]
		SET			Value = newField.Value,
					Updated = newField.Updated

		FROM		@Fields newField
			INNER JOIN	[UnversionedFields] dbField
				ON		dbField.[ItemId]   = newField.[ItemId]
					AND	dbField.[FieldId]  = newField.[FieldId]
					AND	dbField.[Language] = newField.[Language]

		WHERE dbField.Value <> newField.Value COLLATE Latin1_General_CS_AS -- Sitecore.Support.290996

		-- Insert new
		INSERT INTO	[UnversionedFields] ([Id], [ItemId], [FieldId], [Language], [Value], [Created], [Updated])
		SELECT		newField.[Id], 
					newField.[ItemId], 
					newField.[FieldId], 
					newField.[Language], 
					newField.[Value], 
					newField.[Created], 
					newField.[Updated]

		FROM		@Fields newField
			LEFT JOIN	[UnversionedFields] dbField
				ON		dbField.[ItemId]   = newField.[ItemId]
					AND	dbField.[FieldId]  = newField.[FieldId]
					AND	dbField.[Language] = newField.[Language]

		WHERE		newField.[Language] IS NOT NULL
				AND	newField.[Version] IS NULL
				AND	dbField.[Id] IS NULL

		-- Delete removed fields
		DELETE FROM [UnversionedFields]
		FROM		[UnversionedFields] dbField

			-- only for the languages that are being edited
			INNER JOIN (
				SELECT DISTINCT Id, Language
				FROM @Uris) AS langUri
				ON langUri.Id       = dbField.ItemId AND
				   langUri.Language = dbField.Language
			
			LEFT JOIN  @Fields newField
				ON dbField.[ItemId]   = newField.[ItemId] AND
				   dbField.[FieldId]  = newField.[FieldId] AND
				   dbField.[Language] = newField.[Language] AND
				   newField.[Version] IS NULL

		WHERE newField.[ItemId] IS NULL

		------------ Versioned Fields -------------------------

		-- report changes
		IF @CalculateChanges = 1 AND EXISTS (Select 1 from @VariantFieldsToReport)
		BEGIN
			SELECT newField.ItemId, 
				   newField.FieldId,
				   newField.Language, 
				   newField.Version, 
				   newField.Value,
				   dbField.Value AS OriginalValue,
				   CASE 
						WHEN dbField.Id IS NULL THEN 'Created'
						WHEN dbField.Value = newField.Value COLLATE Latin1_General_CS_AS THEN 'Unchanged' -- Sitecore.Support.290996
						ELSE 'Updated' 
				   END AS EditType
			FROM @Fields newField

				INNER JOIN @VariantFieldsToReport reportField
					on reportField.Id = newField.FieldId

				LEFT OUTER JOIN VersionedFields dbField
					ON dbField.ItemId   = newField.ItemId   AND
					   dbField.FieldId  = newField.FieldId  AND
					   dbField.Language = newField.Language AND
					   dbField.Version  = newField.Version
		END

		-- Update existing
		UPDATE		[VersionedFields]
		SET			Value = newField.Value,
					Updated = newField.Updated

		FROM		@Fields newField
			INNER JOIN	[VersionedFields] dbField
				ON		dbField.[ItemId]   = newField.[ItemId]
					AND	dbField.[FieldId]  = newField.[FieldId]
					AND	dbField.[Language] = newField.[Language]
					AND	dbField.[Version]  = newField.[Version]

		WHERE dbField.Value <> newField.Value COLLATE Latin1_General_CS_AS -- Sitecore.Support.290996

		-- Insert new
		INSERT INTO	[VersionedFields] ([Id], [ItemId], [FieldId], [Language], [Version], [Value], [Created], [Updated])
		SELECT		newField.[Id], 
					newField.[ItemId], 
					newField.[FieldId], 
					newField.[Language], 
					newField.[Version], 
					newField.[Value], 
					newField.[Created], 
					newField.[Updated]

		FROM		@Fields newField
			LEFT OUTER JOIN	[VersionedFields] dbField
				ON		dbField.[ItemId]   = newField.[ItemId]
					AND	dbField.[FieldId]  = newField.[FieldId]
					AND	dbField.[Language] = newField.[Language]
					AND	dbField.[Version]  = newField.[Version]

		WHERE  newField.Language IS NOT NULL AND
			   newField.Version IS NOT NULL AND
			   dbField.Id IS NULL

		-- Delete removed fields
		DELETE FROM [VersionedFields]
		FROM		[VersionedFields] dbField

			INNER JOIN @Uris uri
				ON dbField.[ItemId] = uri.[Id] AND
				   dbField.Language = uri.Language AND
				   dbField.Version  = uri.Version

			LEFT JOIN @Fields newField
				ON dbField.[ItemId]   = newField.[ItemId] AND
				   dbField.[FieldId]  = newField.[FieldId] AND
				   dbField.[Language] = newField.[Language] AND
				   dbField.[Version]  = newField.[Version]

		WHERE newField.[ItemId] IS NULL

		-- Because there is a mixture of DML and read queries, this is required in order to force
		-- processing of all previous statements. (PBI-21729)
		SELECT 1

	END TRY
	BEGIN CATCH

		DECLARE @error_number INTEGER = ERROR_NUMBER();
		DECLARE @error_severity INTEGER = ERROR_SEVERITY();
		DECLARE @error_state INTEGER = ERROR_STATE();
		DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @error_procedure SYSNAME = ERROR_PROCEDURE();
		DECLARE @error_line INTEGER = ERROR_LINE();
    
		RAISERROR( N'T-SQL ERROR %d, SEVERITY %d, STATE %d, PROCEDURE %s, LINE %d, MESSAGE: %s', @error_severity, 1, @error_number, @error_severity, @error_state, @error_procedure, @error_line, @error_message ) WITH NOWAIT;
  
	END CATCH;";
      }
    }

    // Variant Data

    public async Task<Tuple<ItemBaseEntity, IEnumerable<FieldDataEntity>>> GetVariantData(IDatabaseConnection connection, IItemVariantIdentifier locator)
    {
      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
                    SourceSchema.Items.Queries.GetItemVariant,
                    new
                    {
                      Id = locator.Id,
                      Version = locator.Version,
                      Language = locator.Language
                    },
                    null,
                    connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDatas = (await results.ReadAsync<ItemBaseEntity>().ConfigureAwait(false)).ToArray();
          var itemData = itemDatas.FirstOrDefault();

          if (itemData == null) return null; // not-found

          var fieldDatas = (await results.ReadAsync<FieldDataEntity>().ConfigureAwait(false)).ToArray();

          return new Tuple<ItemBaseEntity, IEnumerable<FieldDataEntity>>(itemData, fieldDatas);
        }
      }).ConfigureAwait(false);
    }

    public async Task<Tuple<IEnumerable<ItemBaseEntity>, IEnumerable<FieldDataEntity>>> GetVariantsData(
        IDatabaseConnection connection,
        IReadOnlyCollection<IItemVariantIdentifier> uris)
    {
      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
                    SourceSchema.Items.Queries.GetItemVariants,
                    new { Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(uris) },
                    null,
                    connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDatas = (await results.ReadAsync<ItemBaseEntity>().ConfigureAwait(false)).ToArray();

          var fieldDatas =
              (await results.ReadAsync<FieldDataEntity>().ConfigureAwait(false)).ToArray();

          return new Tuple<IEnumerable<ItemBaseEntity>, IEnumerable<FieldDataEntity>>(
              itemDatas,
              fieldDatas);
        }
      }).ConfigureAwait(false);
    }

    public async Task<Tuple<ItemDataEntity, FieldDataChangeEntity[]>> DeleteVariantData(
        IDatabaseConnection connection,
        IItemVariantIdentifier uri,
        IReadOnlyCollection<Guid> variantFieldsToReport,
        bool calculateChanges = true)
    {
      var fieldReportTable = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(variantFieldsToReport);

      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            TargetSchema.Items.Queries.DeleteItemVariant,
            new
            {
              Id = uri.Id,
              Version = uri.Version,
              Language = uri.Language,
              CalculateChanges = calculateChanges ? 1 : 0,
              VariantFieldsToReport = fieldReportTable
            },
            connection.Transaction,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemData = (await results.ReadAsync<ItemDataEntity>().ConfigureAwait(false)).FirstOrDefault();

          var versionedFieldDataChanges = results.IsConsumed ?
              new FieldDataChangeEntity[0] :
              (await results.ReadAsync<FieldDataChangeEntity>().ConfigureAwait(false))
                  .Where(f => f.Value != f.OriginalValue).ToArray();

          return new Tuple<ItemDataEntity, FieldDataChangeEntity[]>(
              itemData,
              versionedFieldDataChanges.ToArray());
        }
      }).ConfigureAwait(false);
    }

    public async Task<Tuple<ItemDataEntity[], FieldDataChangeEntity[]>> DeleteVariantsData(
        IDatabaseConnection connection,
        IReadOnlyCollection<IItemVariantIdentifier> uris,
        IReadOnlyCollection<Guid> variantFieldsToReport,
        bool calculateChanges = true)
    {
      var fieldReportTable = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(variantFieldsToReport);

      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            TargetSchema.Items.Queries.DeleteItemVariants,
            new
            {
              Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(uris),
              CalculateChanges = calculateChanges ? 1 : 0,
              VariantFieldsToReport = fieldReportTable
            },
            connection.Transaction,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDatas = (await results.ReadAsync<ItemDataEntity>().ConfigureAwait(false)).ToArray();

          var versionedFieldDataChanges = results.IsConsumed ?
              new FieldDataChangeEntity[0] :
              (await results.ReadAsync<FieldDataChangeEntity>().ConfigureAwait(false))
                  .Where(f => f.Value != f.OriginalValue).ToArray();

          return new Tuple<ItemDataEntity[], FieldDataChangeEntity[]>(
              itemDatas,
              versionedFieldDataChanges);
        }
      }).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes the item, field and link data for all languages and versions of an item
    /// </summary>
    /// <param name="connection">The database transactional connection.</param>
    /// <param name="id">The id for the item to be deleted.</param>
    /// <param name="calculateChanges">Set to <c></c></param>
    /// <returns>True if successful, otherwise false.</returns>
    public Task<ItemDataDeleteEntity> DeleteItem(IDatabaseConnection connection, Guid id, bool calculateChanges = true)
    {
      return connection.ExecuteAsync(async conn =>
      {
        var results = await conn.QueryAsync<ItemDataDeleteEntity>(
                TargetSchema.Items.Queries.DeleteItem,
                new
                {
                  Id = id,
                  CalculateChanges = calculateChanges ? 1 : 0
                },
                connection.Transaction,
                connection.CommandTimeout).ConfigureAwait(false);

        return results.FirstOrDefault();
      });
    }

    /// <summary>
    /// Removes the item, field and link data for all languages and versions of multiple items.
    /// </summary>
    /// <param name="connection">The database transactional connection.</param>
    /// <param name="ids">The ids of the items to be deleted.</param>
    /// <param name="calculateChanges">Whether changes should be calculated.</param>
    /// <returns>True if successful, otherwise false.</returns>
    public Task<ItemDataDeleteEntity[]> DeleteItems(IDatabaseConnection connection, IReadOnlyCollection<Guid> ids, bool calculateChanges = true)
    {
      return connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            TargetSchema.Items.Queries.DeleteItems,
            new
            {
              Ids = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(ids),
              CalculateChanges = calculateChanges ? 1 : 0
            },
            connection.Transaction,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDataChanges = (await results.ReadAsync<ItemDataDeleteEntity>().ConfigureAwait(false)).ToArray();
          return itemDataChanges;
        }
      });
    }

    // Links
    public Task<IEnumerable<ItemLinkEntity>> GetVariantOutLinks(IDatabaseConnection connection, string source, IItemVariantIdentifier locator)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.QueryAsync<ItemLinkEntity>(
              LinksSchema.Links.Queries.GetItemVariantOutLinks,
              new
              {
                Id = locator.Id,
                Version = locator.Version,
                Language = locator.Language,
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task<IEnumerable<ItemLinkEntity>> GetVariantsOutLinks(
        IDatabaseConnection connection,
        string source,
        IReadOnlyCollection<IItemVariantIdentifier> locators,
        IReadOnlyCollection<Guid> fieldIdsWhiteList)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.QueryAsync<ItemLinkEntity>(
              LinksSchema.Links.Queries.GetItemVariantsOutLinks,
              new
              {
                Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(locators),
                SourceIdentifier = source,
                FieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(fieldIdsWhiteList)
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task<IEnumerable<ItemLinkEntity>> GetVariantInLinks(IDatabaseConnection connection, string source, IItemVariantIdentifier locator)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.QueryAsync<ItemLinkEntity>(
              LinksSchema.Links.Queries.GetItemVariantInLinks,
              new
              {
                Id = locator.Id,
                Version = locator.Version,
                Language = locator.Language,
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task<IEnumerable<ItemLinkEntity>> GetVariantsInLinks(
        IDatabaseConnection connection,
        string source,
        IReadOnlyCollection<IItemVariantIdentifier> locators,
        IReadOnlyCollection<Guid> fieldIdsWhiteList)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.QueryAsync<ItemLinkEntity>(
              LinksSchema.Links.Queries.GetItemVariantsInLinks,
              new
              {
                Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(locators),
                SourceIdentifier = source,
                FieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(fieldIdsWhiteList)
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task<Tuple<IEnumerable<ItemLinkEntity>, IEnumerable<ItemLinkEntity>>> GetVariantsAllLinks(
        IDatabaseConnection connection,
        string source,
        IReadOnlyCollection<IItemVariantIdentifier> locators,
        IReadOnlyCollection<Guid> outFieldIdsWhiteList,
        IReadOnlyCollection<Guid> inFieldIdsWhiteList)
    {
      return connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            LinksSchema.Links.Queries.GetItemVariantsAllLinks,
            new
            {
              Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(locators),
              SourceIdentifier = source,
              OutFieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(outFieldIdsWhiteList),
              InFieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(inFieldIdsWhiteList)
            },
            connection.Transaction,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var outLinks = (await results.ReadAsync<ItemLinkEntity>().ConfigureAwait(false)).ToArray();
          var inLinks = (await results.ReadAsync<ItemLinkEntity>().ConfigureAwait(false)).ToArray();
          return new Tuple<IEnumerable<ItemLinkEntity>, IEnumerable<ItemLinkEntity>>(
              outLinks,
              inLinks);
        }
      });
    }

    public Task<IEnumerable<ItemLinkEntity>> GetOutRelationships(
        IDatabaseConnection connection,
        string source,
        IReadOnlyCollection<Guid> fieldIdsWhiteList)
    {
      return connection.ExecuteAsync(async conn =>
      await conn.QueryAsync<ItemLinkEntity>(
          LinksSchema.Links.Queries.GetItemOutIdsBySourceField,
          new
          {
            SourceIdentifier = source,
            FieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(fieldIdsWhiteList)
          },
          connection.Transaction,
          connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task<IEnumerable<ItemLinkEntity>> GetInRelationships(
        IDatabaseConnection connection,
        string source,
        IReadOnlyCollection<Guid> outItemsIds,
        IReadOnlyCollection<Guid> fieldIdsWhiteList)
    {
      return connection.ExecuteAsync(async conn =>
      await conn.QueryAsync<ItemLinkEntity>(
          LinksSchema.Links.Queries.GetItemInIds,
          new
          {
            SourceIdentifier = source,
            OutItemsIds = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(outItemsIds),
            FieldIdsWhiteList = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(fieldIdsWhiteList)
          },
          connection.Transaction,
          connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task AddOrUpdateLinks(IDatabaseConnection connection, string source, IReadOnlyCollection<ItemLinkEntity> links)
    {
      var linksTable = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildLinkDataTable(links, source);

      return connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            LinksSchema.Links.Queries.SetItemVariantsLinks,
            new { Links = linksTable },
            connection.Transaction,
            connection.CommandTimeout).ConfigureAwait(false))
        {
        }
      });
    }

    public Task DeleteVariantLinks(IDatabaseConnection connection, string source, IItemVariantIdentifier uri)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              LinksSchema.Links.Queries.DeleteItemVariantLinks,
              new
              {
                Id = uri.Id,
                Version = uri.Version,
                Language = uri.Language,
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public Task DeleteVariantsLinks(IDatabaseConnection connection, string source, IReadOnlyCollection<IItemVariantIdentifier> uris)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              LinksSchema.Links.Queries.DeleteItemVariantsLinks,
              new
              {
                Uris = Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerUtils.BuildUriTable(uris),
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    public async Task DeleteItemLinks(IDatabaseConnection connection, string source, Guid id)
    {
      await connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              LinksSchema.Links.Queries.DeleteItemLinks,
              new
              {
                Id = id,
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false)).ConfigureAwait(false);
    }

    public async Task DeleteItemsLinks(IDatabaseConnection connection, string source, IReadOnlyCollection<Guid> ids)
    {
      await connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              LinksSchema.Links.Queries.DeleteItemsLinks,
              new
              {
                Ids = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(ids),
                SourceIdentifier = source
              },
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false)).ConfigureAwait(false);
    }

    // Item Nodes

    public async Task<IEnumerable<ItemDataEntity>> GetItemNodeAncestorsData(IDatabaseConnection connection, Guid id)
    {
      return await connection.ExecuteAsync(async conn =>
          await conn.QueryAsync<ItemDataEntity>(
              SourceSchema.Items.Queries.GetItemNodeAncestors,
              new { Id = id },
              commandTimeout: connection.CommandTimeout).ConfigureAwait(false)).ConfigureAwait(false);
    }

    public async Task<Tuple<ItemBaseEntity, IEnumerable<FieldDataEntity>>> GetItemNodeData(IDatabaseConnection connection, Guid id)
    {
      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            SourceSchema.Items.Queries.GetItemNode,
            new
            {
              Id = id,
              RevisionId = PublishingConstants.StandardFields.Revision
            },
            null,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDatas = (await results.ReadAsync<ItemBaseEntity>().ConfigureAwait(false)).ToArray();
          var itemData = itemDatas.FirstOrDefault();

          if (itemData == null) return null; // not-found

          var fieldDatas =
              (await results.ReadAsync<FieldDataEntity>().ConfigureAwait(false)).ToArray();

          var relationships = Enumerable.Empty<ItemLinkEntity>();

          return new Tuple<ItemBaseEntity, IEnumerable<FieldDataEntity>>(itemData, fieldDatas);
        }
      }).ConfigureAwait(false);
    }

    public async Task<Tuple<IEnumerable<ItemBaseEntity>, IEnumerable<FieldDataEntity>>> GetItemNodesData(IDatabaseConnection connection, IReadOnlyCollection<Guid> ids)
    {
      return await connection.ExecuteAsync(async conn =>
      {
        using (var results = await conn.QueryMultipleAsync(
            SourceSchema.Items.Queries.GetItemNodes,
            new
            {
              Ids = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(ids),
              RevisionId = PublishingConstants.StandardFields.Revision
            },
            null,
            connection.CommandTimeout).ConfigureAwait(false))
        {
          var itemDatas = (await results.ReadAsync<ItemBaseEntity>().ConfigureAwait(false)).ToArray();

          var fieldDatas = (await results.ReadAsync<FieldDataEntity>().ConfigureAwait(false)).ToArray();

          return new Tuple<IEnumerable<ItemBaseEntity>, IEnumerable<FieldDataEntity>>(itemDatas, fieldDatas);
        }
      }).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ItemDescriptorEntity>> GetAllItems(IDatabaseConnection connection)
    {
      return await connection.ExecuteAsync<IEnumerable<ItemDescriptorEntity>>(async conn =>
      {
        var results = await conn.QueryAsync<ItemDescriptorEntity>(
            SourceSchema.Items.Queries.GetAllNodes,
            new { BaseTemplateFieldId = PublishingConstants.TemplateFields.BaseTemplate },
            null,
            connection.CommandTimeout).ConfigureAwait(false);

        return results;
      }).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ItemDescriptorEntity>> GetItemsByIds(IDatabaseConnection connection, IReadOnlyCollection<Guid> ids)
    {
      return await connection.ExecuteAsync<IEnumerable<ItemDescriptorEntity>>(async conn =>
      {
        var results = await conn.QueryAsync<ItemDescriptorEntity>(
            SourceSchema.Items.Queries.GetNodesByIds,
            new { Ids = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(ids), BaseTemplateFieldId = PublishingConstants.TemplateFields.BaseTemplate },
            null,
            connection.CommandTimeout).ConfigureAwait(false);

        return results;
      }).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ItemDescriptorEntity>> GetItemsByTemplateId(IDatabaseConnection connection, Guid templateId)
    {
      return await connection.ExecuteAsync<IEnumerable<ItemDescriptorEntity>>(async conn =>
      {
        var results = await conn.QueryAsync<ItemDescriptorEntity>(
            SourceSchema.Items.Queries.GetNodesByTemplateId,
            new { TemplateId = templateId, BaseTemplateFieldId = PublishingConstants.TemplateFields.BaseTemplate },
            null,
            connection.CommandTimeout).ConfigureAwait(false);

        return results;
      }).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ItemDescriptorEntity>> GetChildItemsByParentIds(IDatabaseConnection connection, IReadOnlyCollection<Guid> parentIds)
    {
      return await connection.ExecuteAsync<IEnumerable<ItemDescriptorEntity>>(async conn =>
      {
        var results = await conn.QueryAsync<ItemDescriptorEntity>(
            SourceSchema.Items.Queries.GetNodesByParentIds,
            new { Ids = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(parentIds), BaseTemplateFieldId = PublishingConstants.TemplateFields.BaseTemplate },
            null,
            connection.CommandTimeout).ConfigureAwait(false);

        return results;
      }).ConfigureAwait(false);
    }

    public Task InitialiseDataQueryParams(IDatabaseConnection connection, IReadOnlyCollection<Language> languageFilter, IReadOnlyCollection<Guid> fieldFilter)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              SourceSchema.Params.Queries.InitialiseDataQueryParams,
              new
              {
                FieldIds = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildIdTable(fieldFilter),
                Languages = Sitecore.Framework.Publishing.Sql.SqlServerUtils.BuildStringTable(languageFilter.Select(l => l.Name))
              },
              null,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    // Descendants

    public Task RebuildDescendants(IDatabaseConnection connection)
    {
      return connection.ExecuteAsync(async conn =>
          await conn.ExecuteAsync(
              TargetSchema.Descendants.Queries.RebuildDescendants,
              null,
              connection.Transaction,
              connection.CommandTimeout).ConfigureAwait(false));
    }

    private async Task<Tuple<ItemDataChangeEntity[], FieldDataChangeEntity[]>> GetChangeReport(
        SqlMapper.GridReader reader,
        IReadOnlyCollection<Guid> fields,
        IReadOnlyCollection<Guid> langVariantFields,
        IReadOnlyCollection<Guid> variantFields)
    {
      var itemDataChanges = (await reader.ReadAsync<ItemDataChangeEntity>().ConfigureAwait(false)).ToArray();

      var invariantFieldDataChanges = !fields.Any() ?
          new FieldDataChangeEntity[0] :
          (await reader.ReadAsync<FieldDataChangeEntity>().ConfigureAwait(false))
              .Where(f => f.Value != f.OriginalValue) // don't report fields that didnt change.
              .ToArray();

      var langVariantFieldDataChanges = !langVariantFields.Any() ?
          new FieldDataChangeEntity[0] :
          (await reader.ReadAsync<FieldDataChangeEntity>().ConfigureAwait(false))
              .Where(f => f.Value != f.OriginalValue)
              .ToArray();

      var variantFieldDataChanges = !variantFields.Any() ?
          new FieldDataChangeEntity[0] :
          (await reader.ReadAsync<FieldDataChangeEntity>().ConfigureAwait(false))
              .Where(f => f.Value != f.OriginalValue)
              .ToArray();

      return new Tuple<ItemDataChangeEntity[], FieldDataChangeEntity[]>(
              itemDataChanges,
              invariantFieldDataChanges.Concat(langVariantFieldDataChanges).Concat(variantFieldDataChanges).ToArray());
    }


  }
}