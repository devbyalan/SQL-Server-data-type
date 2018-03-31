-- =============================================
-- Author:		Alan M. Fortuna R.
-- Create date: 30/03/2018
DECLARE @schemaName VARCHAR(50) = '',
	    @tableName VARCHAR(50) = '',
		@objectName VARCHAR(50) = '';		
SET @objectName = @schemaName + '.' + @tableName

-- =============================================
/*####################################################################################################################
--> Step #1: Delete all hypotetical index.
####################################################################################################################*/
/*
--> Some info: 1 - https://www.brentozar.com/blitz/hypothetical-indexes-index-tuning-wizard/
			   2 - https://www.red-gate.com/simple-talk/sql/database-administration/hypothetical-indexes-on-sql-server/   
			   3 - https://technet.microsoft.com/en-us/library/ms190172(v=sql.105).aspx
*/
DECLARE @t_sql VARCHAR(MAX) = '';
DECLARE db_cursor CURSOR FOR
	WITH hi AS 
	(
		SELECT QUOTENAME(SCHEMA_NAME(o.[schema_id])) +'.'+ QUOTENAME(OBJECT_NAME(i.[object_id])) AS [Table] , QUOTENAME([i].[name]) AS [Index_or_Statistics], 1 AS [Type]
		FROM sys.indexes AS [i]
		JOIN sys.objects AS [o]
		ON i.object_id = o.[object_id]
		WHERE 1=1 
			AND INDEXPROPERTY(i.[object_id], i.[name], 'IsHypothetical') = 1
			AND OBJECTPROPERTY([o].[object_id], 'IsUserTable') = 1

		UNION ALL

		SELECT QUOTENAME(SCHEMA_NAME(o.[schema_id])) +'.'+ QUOTENAME(OBJECT_NAME(o.[object_id])) AS [Table], QUOTENAME([s].[name]) AS [Index_or_Statistics], 2 AS [Type]
		FROM sys.stats AS [s]
		JOIN sys.objects AS [o] ON [o].[object_id] = [s].[object_id]
		WHERE  1=1 
			AND [s].[user_created] = 0
			AND [o].[name] LIKE '[_]dta[_]%'
			AND OBJECTPROPERTY([o].[object_id], 'IsUserTable') = 1
	)
	SELECT 
		   --[hi].[Table] ,
		   --[hi].[Index_or_Statistics] ,
		   CASE [hi].[Type] 
			   WHEN 1 THEN 'DROP INDEX ' + [hi].[Index_or_Statistics] + ' ON ' + [hi].[Table] + ';'
			   WHEN 2 THEN 'DROP STATISTICS ' + hi.[Table] + '.' + hi.[Index_or_Statistics] + ';'
			   ELSE 'DEAR GOD WHAT HAVE YOU DONE?'
		   END AS t_sql--[T-SQL Drop Command]
	FROM [hi]
OPEN db_cursor  
	FETCH NEXT FROM db_cursor INTO @t_sql
		WHILE @@FETCH_STATUS = 0  
			BEGIN 
				PRINT @t_sql
				EXEC (@t_sql)
				FETCH NEXT FROM db_cursor INTO @t_sql 
			END 
	CLOSE db_cursor  
DEALLOCATE db_cursor 

/*####################################################################################################################
--> Step #2: generate script for all index (Cluster, NonCluster, filtered, unique, etc.)
####################################################################################################################*/
/*
 Script out indexes completely, including both PK's and regular indexes, each clustered or nonclustered.
 DOES NOT HANDLE COMPRESSION; that's ok, since 2008 R2 RTM benchmarking shows it's faster and results in smaller indexes to insert uncompressed and then compress later
 HARDCODES [dbo] schema (i.e. it doesn't say [JohnDoe].[table], changing that to [dbo].[table]
 originally from http://www.sqlservercentral.com/Forums/Topic961088-2753-2.aspx
*/
DECLARE @idxTableName SYSNAME,
		@idxTableID INT,
		@idxname SYSNAME,
		@idxid INT,
		@colCount INT,
		@IxColumn SYSNAME,
		@IxFirstColumn BIT,
		@ColumnIDInTable INT,
		@ColumnIDInIndex INT,
		@IsIncludedColumn INT,
		@sIncludeCols VARCHAR(MAX),
		@sIndexCols VARCHAR(MAX),
		@sSQL VARCHAR(MAX),
		@sParamSQL VARCHAR(MAX),
		@sFilterSQL VARCHAR(MAX),
		@location SYSNAME,
		@IndexCount INT,
		@CurrentIndex INT,
		@CurrentCol INT,
		@Name VARCHAR(128),
		@IsPrimaryKey TINYINT,
		@Fillfactor INT,
		@FilterDefinition VARCHAR(MAX),
		@IsClustered BIT -- used solely for putting information into the result table
		
IF OBJECT_ID('tempdb..#IndexSQL') IS NOT NULL
	BEGIN
		DROP TABLE [dbo].[#IndexSQL];
	END

CREATE TABLE #IndexSQL
( 
	 TableName VARCHAR(128) NOT NULL
	,IndexName VARCHAR(128) NOT NULL
	,IsClustered BIT NOT NULL
	,IsPrimaryKey BIT NOT NULL
	,IndexCreateSQL VARCHAR(max) NOT NULL
)
IF OBJECT_ID('tempdb..#IndexListing') IS NOT NULL
	BEGIN
		DROP TABLE [dbo].[#IndexListing];
	END

CREATE TABLE #IndexListing
(
	[IndexListingID] INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	[TableName] SYSNAME COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ObjectID] INT NOT NULL,
	[IndexName] SYSNAME COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IndexID] INT NOT NULL,
	[IsPrimaryKey] TINYINT NOT NULL,
	[FillFactor] INT,
	[FilterDefinition] NVARCHAR(MAX) NULL
)

IF OBJECT_ID('tempdb..#ColumnListing') IS NOT NULL
	BEGIN
		DROP TABLE [dbo].[#ColumnListing];
	END

CREATE TABLE #ColumnListing
(
	[ColumnListingID] INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	[ColumnIDInTable] INT NOT NULL,
	[Name] SYSNAME COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ColumnIDInIndex] INT NOT NULL,
	[IsIncludedColumn] BIT NULL
)

INSERT INTO #IndexListing( [TableName], [ObjectID], [IndexName], [IndexID], [IsPrimaryKey], [FILLFACTOR], [FilterDefinition] )
SELECT OBJECT_NAME(si.object_id),
	   si.object_id,
	   si.name, 
	   si.index_id, 
	   si.Is_Primary_Key, 
	   si.Fill_Factor, 
	   si.filter_definition
FROM sys.indexes si
LEFT OUTER JOIN information_schema.table_constraints tc ON si.name = tc.constraint_name AND OBJECT_NAME(si.object_id) = tc.table_name
WHERE OBJECTPROPERTY(si.object_id, 'IsUserTable') = 1
ORDER BY OBJECT_NAME(si.object_id), si.index_id

SELECT @IndexCount = @@ROWCOUNT,
	   @CurrentIndex = 1

WHILE @CurrentIndex <= @IndexCount
	BEGIN /*main begin*/
		SELECT  @idxTableName = [TableName],
				@idxTableID = [ObjectID],
				@idxname = [IndexName],
				@idxid = [IndexID],
				@IsPrimaryKey = [IsPrimaryKey],
				@FillFactor = [FILLFACTOR],
				@FilterDefinition = [FilterDefinition]
		FROM #IndexListing
		WHERE [IndexListingID] = @CurrentIndex

		-- So - it is either an index or a constraint
		-- Check if the index is unique
		IF (@IsPrimaryKey = 1)
			BEGIN
				SET @sSQL = 'ALTER TABLE [dbo].[' + @idxTableName + '] ADD CONSTRAINT [' + @idxname + '] PRIMARY KEY '
	
				-- Check if the index is clustered
				IF (INDEXPROPERTY(@idxTableID, @idxname, 'IsClustered') = 0)
					BEGIN
						SET @sSQL = @sSQL + 'NON';
						SET @IsClustered = 0;
					END
				ELSE
					BEGIN
						SET @IsClustered = 1;
					END
				SET @sSQL = @sSQL + 'CLUSTERED' + CHAR(13) + '(' + CHAR(13);
			END
		ELSE
			BEGIN
				SET @sSQL = 'CREATE '
				-- Check if the index is unique
				IF (INDEXPROPERTY(@idxTableID, @idxname, 'IsUnique') = 1)
					BEGIN
						SET @sSQL = @sSQL + 'UNIQUE ';
					END
				-- Check if the index is clustered
				IF (INDEXPROPERTY(@idxTableID, @idxname, 'IsClustered') = 1)
					BEGIN
						SET @sSQL = @sSQL + 'CLUSTERED ';
						SET @IsClustered = 1;
					END
				ELSE
					BEGIN
						SET @IsClustered = 0;
					END

				SELECT @sSQL = @sSQL + 'INDEX [' + @idxname + '] ON [dbo].[' + @idxTableName + ']' + CHAR(13) + '(' + CHAR(13),  @colCount = 0
			END

		-- Get the number of cols in the index
		SELECT @colCount = COUNT(*)
		FROM sys.index_columns ic
		INNER JOIN sys.columns sc ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
		WHERE ic.object_id = @idxtableid AND index_id = @idxid AND ic.is_included_column = 0

		-- Get the file group info
		SELECT @location = f.[name]
		FROM sys.indexes i
		INNER JOIN sys.filegroups f ON i.data_space_id = f.data_space_id
		INNER JOIN sys.all_objects o ON i.[object_id] = o.[object_id]
		WHERE o.object_id = @idxTableID AND i.index_id = @idxid

		-- Get all columns of the index
		INSERT INTO #ColumnListing( [ColumnIDInTable], [Name], [ColumnIDInIndex],[IsIncludedColumn] )
		SELECT sc.column_id, 
			   sc.name, 
			   ic.index_column_id, 
			   ic.is_included_column
		FROM sys.index_columns ic
		INNER JOIN sys.columns sc ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
		WHERE ic.object_id = @idxTableID AND index_id = @idxid
		ORDER BY ic.index_column_id

		IF @@ROWCOUNT > 0
			BEGIN	 
				SELECT @IxFirstColumn = 1,
					   @sIncludeCols = '', 
					   @sIndexCols = '', 
					   @CurrentCol = 1

				WHILE @CurrentCol <= @ColCount
					BEGIN /*begin while*/
						SELECT  @ColumnIDInTable = ColumnIDInTable,
								@Name = Name,
								@ColumnIDInIndex = ColumnIDInIndex,
								@IsIncludedColumn = IsIncludedColumn
						FROM #ColumnListing
						WHERE [ColumnListingID] = @CurrentCol

						IF @IsIncludedColumn = 0
							BEGIN
								SET @sIndexCols = CHAR(9) + @sIndexCols + '[' + @Name + '] ';

								-- Check the sort order of the index cols ????????
								IF (INDEXKEY_PROPERTY (@idxTableID,@idxid,@ColumnIDInIndex,'IsDescending')) = 0
									BEGIN
										SET @sIndexCols = @sIndexCols + ' ASC ';
									END
								ELSE
									BEGIN
										SET @sIndexCols = @sIndexCols + ' DESC ';
									END

								IF @CurrentCol < @colCount
									BEGIN
										SET @sIndexCols = @sIndexCols + ', '
									END
							END
						ELSE
							BEGIN
								-- Check for any include columns
								IF LEN(@sIncludeCols) > 0
									BEGIN
										SET @sIncludeCols = @sIncludeCols + ',';
									END
								SET @sIncludeCols = @sIncludeCols + '[' + @IxColumn + ']';
							END

						SET @CurrentCol += 1;
					END	/*end while*/

				TRUNCATE TABLE #ColumnListing;

				--append to the result
				IF LEN(@sIncludeCols) > 0
					BEGIN
						SET @sIndexCols = @sSQL + @sIndexCols + CHAR(13) + ') ' + ' INCLUDE ( ' + @sIncludeCols + ' ) ';
					END
				ELSE
					BEGIN
						SET @sIndexCols = @sSQL + @sIndexCols + CHAR(13) + ') ';
					END
				-- Add filtering
				IF @FilterDefinition IS NOT NULL
					BEGIN
						SET @sFilterSQL = ' WHERE ' + @FilterDefinition + ' ' + CHAR(13);
					END
				ELSE
					BEGIN
						SET @sFilterSQL = '';
					END

				-- Build the options
				SET @sParamSQL = 'WITH ( PAD_INDEX = ';

				IF INDEXPROPERTY(@idxTableID, @idxname, 'IsPadIndex') = 1
					BEGIN
						SET @sParamSQL = @sParamSQL + 'ON,';
					END
				ELSE
					BEGIN
						SET @sParamSQL = @sParamSQL + 'OFF,';
					END
				SET @sParamSQL = @sParamSQL + ' ALLOW_PAGE_LOCKS = ';

				IF INDEXPROPERTY(@idxTableID, @idxname, 'IsPageLockDisallowed') = 0
					BEGIN
						SET @sParamSQL = @sParamSQL + 'ON,';
					END
				ELSE
					BEGIN
						SET @sParamSQL = @sParamSQL + 'OFF,';
					END

				SET @sParamSQL = @sParamSQL + ' ALLOW_ROW_LOCKS = ';

				IF INDEXPROPERTY(@idxTableID, @idxname, 'IsRowLockDisallowed') = 0
					BEGIN
						SET @sParamSQL = @sParamSQL + 'ON,'
					END
				ELSE
					BEGIN
						SET @sParamSQL = @sParamSQL + 'OFF,';
					END
				SET @sParamSQL = @sParamSQL + ' STATISTICS_NORECOMPUTE = ';

				-- THIS DOES NOT WORK PROPERLY; IsStatistics only says what generated the last set, not what it was set to do.
				IF (INDEXPROPERTY(@idxTableID, @idxname, 'IsStatistics') = 1)
					BEGIN
						SET @sParamSQL = @sParamSQL + 'ON';
					END
				ELSE
					BEGIN
						SET @sParamSQL = @sParamSQL + 'OFF';
					END

				-- Fillfactor 0 is actually not a valid percentage on SQL 2008 R2
				IF ISNULL( @FillFactor, 90 ) <> 0 
					BEGIN
						SET @sParamSQL = @sParamSQL + ' ,FILLFACTOR = ' + CAST( ISNULL( @FillFactor, 90 ) AS VARCHAR(3) );
					END

				IF (@IsPrimaryKey = 1) -- DROP_EXISTING isn't valid for PK's
					BEGIN
						SET @sParamSQL = @sParamSQL + ' ) ';
					END
				ELSE
					BEGIN
						SET @sParamSQL = @sParamSQL + ' ,DROP_EXISTING = ON ) ';
					END
				SET @sSQL = @sIndexCols + CHAR(13) + @sFilterSQL + CHAR(13) + @sParamSQL;

				-- 2008 R2 allows ON [filegroup] for primary keys as well, negating the old "IF THE INDEX IS NOT A PRIMARY KEY - ADD THIS - ELSE DO NOT" IsPrimaryKey IF statement
				SET @sSQL = @sSQL + ' ON [' + @location + ']';

				--PRINT @sIndexCols + CHAR(13)
				INSERT INTO #IndexSQL (TableName, IndexName, IsClustered, IsPrimaryKey, IndexCreateSQL) 
				VALUES (@idxTableName, @idxName, @IsClustered, @IsPrimaryKey, @sSQL);
			END
		SET @CurrentIndex = @CurrentIndex + 1
	END/*main end*/

/*####################################################################################################################
--> Step #3: Generate script for default constrarints, check constraints and foreign keys.
####################################################################################################################*/
;WITH ChangeObjects
AS
(
	select 
		   a.[name] as ObjectName,
		   OBJECT_SCHEMA_NAME(b.object_id)+ '.' + b.[name] as [MainTableName],
		   e.[name] as [MainColumnName],
		   tp.[name] AS [MainColumnTypeName],
		   e.is_nullable as [AllowNull],
		   OBJECT_SCHEMA_NAME(c.object_id)+ '.' + c.[name] AS [ForeignTableName],
		   e.[name] AS [ForeignColumn],
		   NULL AS [Definition],
		   a.is_system_named AS [IsSystemNamed],
		   '3_FK' AS DefinitionType
	from sys.foreign_keys as a
	JOIN sys.tables AS B ON  A.parent_object_id = B.object_id
	JOIN sys.tables AS C ON  A.referenced_object_id = C.object_id
	JOIN sys.foreign_key_columns AS D on a.object_id = d.constraint_object_id
	JOIN sys.columns as E on E.object_id = A.parent_object_id
		and D.parent_column_id = E.column_id
	join sys.types as tp on e.system_type_id = tp.system_type_id 
	join sys.columns as f on b.object_id = f.object_id
		and d.referenced_column_id = f.column_id
	where 1=1
	UNION ALL
	 /*Check Constrarint*/
	select a.[name] as ObjectName,
		   OBJECT_SCHEMA_NAME(b.object_id)+ '.' + b.[name] as [MainTableName],
		   c.[name] AS [MainColumnName],
		   tp.[name] AS [MainColumnTypeName],
		   c.is_nullable as [AllowNull],
		   NULL AS [ForeignTableName], 
		   NULL AS [ForeignColumn],
		   a.[definition] AS [Definition],
		   a.is_system_named AS [IsSystemNamed],
		   '1_CK' AS DefinitionType
	from sys.check_constraints as a
	join sys.tables as b on a.parent_object_id = b.object_id
	join sys.columns as c on b.object_id = c.object_id
		and a.parent_column_id = c.column_id
	left join sys.types as tp on c.system_type_id = tp.system_type_id 
	where 1=1
	UNION ALL
	select 
		   df.[name] as ObjectName,
		   OBJECT_SCHEMA_NAME(t.object_id)+ '.' + T.[name] as [MainTableName],
		   c.[name] AS [MainColumnName],
		   tp.[name] AS [MainColumnTypeName],
		   c.is_nullable as [AllowNull],
		   NULL AS [ForeignTableName], 	   
		   NULL AS [ForeignColumn],
		   df.[definition] AS [Definition],
		   df.is_system_named AS [IsSystemNamed],
		   '2_DF' AS [DefinitionType]
	from sys.columns as c
	join sys.tables as t on c.object_id = t.object_id
	join sys.types as tp on c.system_type_id = tp.system_type_id 
	join sys.default_constraints df on t.object_id = df.parent_object_id and df.parent_column_id = c.column_id
	where 1=1
)

--
SELECT 
		A.ObjectName,
		A.MainTableName,
		A.MainColumnName,
		A.MainColumnTypeName,
		A.AllowNull,	
		A.[ForeignTableName], 	   
		A.[ForeignColumn],
		A.[Definition],
		A.[IsSystemNamed],
		A.[DefinitionType],
		A.DropSQL,
		A.CreateSQL
FROM (
	SELECT  CAST(A.ObjectName AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS ObjectName,
			CAST(A.[MainTableName] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [MainTableName],
			CAST(A.[MainColumnName] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [MainColumnName],
			CAST(A.[MainColumnTypeName] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [MainColumnTypeName],
			CAST(A.[AllowNull] AS BIT) AS [AllowNull],
			CAST(A.[ForeignTableName] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [ForeignTableName], 	   
			CAST(A.[ForeignColumn] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [ForeignColumn],
			CAST(A.[Definition] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [Definition],
			CAST(A.[IsSystemNamed] AS BIT) AS [IsSystemNamed],
			CAST(A.[DefinitionType] AS VARCHAR(100)) COLLATE Latin1_General_CI_AS AS [DefinitionType],
			CASE A.[DefinitionType] 
				WHEN '1_CK' THEN 'ALTER TABLE ' + A.[MainTableName] +' DROP CONSTRAINT ' + A.ObjectName
				WHEN '2_DF' THEN 'ALTER TABLE ' + A.[MainTableName] +' DROP CONSTRAINT ' + A.ObjectName
				WHEN '3_FK' THEN 'ALTER TABLE ' + A.[MainTableName] +' DROP CONSTRAINT ' + A.ObjectName
			END  COLLATE Latin1_General_CI_AS AS  DropSQL,
			CASE A.[DefinitionType]   --' ADD CONSTRAINT ' + REPLACE(A.ObjectName,'-','_') +
				WHEN '1_CK' THEN 'ALTER TABLE ' + A.[MainTableName] + ' WITH CHECK ADD CONSTRAINT ' + REPLACE(A.ObjectName,'-','_') + ' CHECK ' + A.[Definition]
				WHEN '2_DF' THEN 'ALTER TABLE ' + A.[MainTableName] + ' ADD CONSTRAINT ' + REPLACE(A.ObjectName,'-','_') + ' DEFAULT ' + A.[Definition]
				WHEN '3_FK' THEN 'ALTER TABLE ' + A.[MainTableName] + ''+ (SELECT   CASE WHEN X.is_not_trusted = 0 then ' WITH CHECK ADD CONSTRAINT ' ELSE ' WITH NOCHECK ADD CONSTRAINT ' END +
																				 x.name +
																				' FOREIGN KEY (' +
																				STUFF(( SELECT ', [' + E.name +']'
																						FROM sys.foreign_keys AS A
																						JOIN sys.foreign_key_columns AS D on a.object_id = d.constraint_object_id
																						JOIN sys.columns AS E on E.object_id = A.parent_object_id
																							AND D.parent_column_id = E.column_id
																						WHERE A.object_ID = x.Object_id
																						FOR XML PATH('')),1,1,'') + ') REFERENCES ' + A.[ForeignTableName] + ' ('+
																				STUFF(( SELECT ', [' + E.name +']'
																						FROM sys.foreign_keys AS A
																						JOIN sys.foreign_key_columns AS D on a.object_id = d.constraint_object_id
																						JOIN sys.columns AS E on E.object_id = A.referenced_object_id
																							AND D.referenced_column_id = E.column_id
																						WHERE A.object_ID = x.Object_id
																						FOR XML PATH('')),1,1,'') +	') '+	
																				CASE WHEN X.update_referential_action <> 0 THEN ' ON UPDATE '+ REPLACE(x.update_referential_action_desc,'_',' ') ELSE '' END COLLATE Latin1_General_CI_AS +
																				CASE WHEN X.delete_referential_action <> 0 THEN ' ON DELETE '+ REPLACE(x.delete_referential_action_desc,'_',' ') ELSE '' END COLLATE Latin1_General_CI_AS +
																				CASE WHEN x.is_not_for_replication = 1 THEN ' NOT FOR REPLICATION ' ELSE '' END
																		FROM sys.foreign_keys AS X
																		WHERE A.ObjectName = X.name) 
			END  COLLATE Latin1_General_CI_AS AS CreateSQL
	FROM ChangeObjects AS A

	UNION ALL
	SELECT  A.IndexName AS ObjectName,
			OBJECT_SCHEMA_NAME(object_id(A.TableName))+ '.' + A.TableName AS MainTableName,
			CAST(NULL AS VARCHAR) AS MainColumnName,
			CAST(NULL AS VARCHAR) AS MainColumnTypeName,
			CAST(0 AS BIT) AS AllowNull,	
			CAST(NULL AS VARCHAR) AS [ForeignTableName], 	   
			CAST(NULL AS VARCHAR) AS [ForeignColumn],
			CAST(NULL AS VARCHAR) AS [Definition],
			CAST(0 AS BIT) AS [IsSystemNamed],
			'0_IX' AS [DefinitionType],
			CASE WHEN (A.IsClustered = 1 AND A.IsPrimaryKey = 1) OR INDEXPROPERTY(OBJECT_ID(A.TableName), A.IndexName, 'IsUnique') = 1 THEN  'ALTER TABLE ' + A.TableName + ' DROP CONSTRAINT ' + A.IndexName ELSE 'DROP INDEX ' + A.IndexName +' ON '+ A.TableName END AS DropSQL,
			A.IndexCreateSQL AS CreateSQL
	FROM #IndexSQL AS A	
)AS A
WHERE 1=1	
	AND @objectName IN (A.MainTableName, A.ForeignTableName)	
ORDER BY  A.[DefinitionType], A.MainTableName