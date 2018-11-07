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
using Sitecore.Framework.Publishing.Data.Classic.SqlServer;
using Sitecore.Framework.Publishing;

namespace Sitecore.Support.Framework.Publishing.Data.Classic.SqlServer
{
  public class ClassicSqlServerDataProvider: Sitecore.Framework.Publishing.Data.Classic.SqlServer.ClassicSqlServerDataProvider
  {
    private readonly ILogger<ClassicSqlServerDataProvider> _logger;

    public ClassicSqlServerDataProvider(ILogger<ClassicSqlServerDataProvider> logger): base(logger)
    {
      Condition.Requires(logger, nameof(logger)).IsNotNull();

      _logger = logger;
    }

    public object SqlServerUtils { get; private set; }

    public async Task<Tuple<ItemDataChangeEntity[], FieldDataChangeEntity[]>> AddOrUpdateVariants(
    IDatabaseConnection connection,
    IReadOnlyCollection<ItemDataEntity> itemDatas,
    IReadOnlyCollection<FieldDataEntity> itemFields,
    IReadOnlyCollection<Guid> invariantFieldsToReport,
    IReadOnlyCollection<Guid> langVariantFieldsToReport,
    IReadOnlyCollection<Guid> variantFieldsToReport,
    bool calculateChanges = true)
    {
      var itemTable = ClassicSqlServerUtils.BuildItemDataTable(itemDatas);
      var fieldsTable = ClassicSqlServerUtils.BuildFieldDataTable(itemFields);

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
						WHEN dbField.Value = newField.Value THEN 'Unchanged'
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
		
		WHERE dbField.Value <> newField.Value

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
						WHEN dbField.Value = newField.Value THEN 'Unchanged'
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

		WHERE dbField.Value <> newField.Value

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
						WHEN dbField.Value = newField.Value THEN 'Unchanged'
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

		WHERE dbField.Value <> newField.Value

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
  }
}