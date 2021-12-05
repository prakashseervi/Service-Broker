USE master
GO
IF EXISTS(SELECT * FROM sys.databases WHERE name = 'TestScheduledJobs')
	DROP DATABASE TestScheduledJobs
GO
CREATE DATABASE TestScheduledJobs
GO
ALTER DATABASE TestScheduledJobs SET ENABLE_BROKER
GO

USE TestScheduledJobs
GO

IF object_id('ScheduledJobs') IS NOT NULL
	DROP TABLE ScheduledJobs

GO	
CREATE TABLE [dbo].[ScheduledJobs]
(
[ID] [bigint] NOT NULL IDENTITY(1, 1),
[ScheduledSql] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[FirstRunOn] [datetime] NOT NULL,
[LastRunOn] [datetime] NULL,
[LastRunOK] [bit] NOT NULL CONSTRAINT [DF__Scheduled__LastR__1C7EB0F9] DEFAULT ((0)),
[IsRepeatable] [bit] NOT NULL CONSTRAINT [DF__Scheduled__IsRep__1D72D532] DEFAULT ((0)),
[IsEnabled] [bit] NOT NULL CONSTRAINT [DF__Scheduled__IsEna__1E66F96B] DEFAULT ((0)),
[ConversationHandle] [uniqueidentifier] NULL,
[CallForID] [int] NOT NULL,
[SrNo] [smallint] NULL,
[Error] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[Remarks] [nvarchar] (500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[NxtRunOn] [datetime] NULL,
[DeleteTime] [datetime] NULL,
[SPID] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


IF object_id('ScheduledJobsErrors') IS NOT NULL
	DROP TABLE ScheduledJobsErrors	
CREATE TABLE [dbo].[ScheduledJobsErrors]
(
[Id] [bigint] NOT NULL IDENTITY(1, 1),
[ErrorLine] [int] NULL,
[ErrorNumber] [int] NULL,
[ErrorMessage] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[ErrorSeverity] [int] NULL,
[ErrorState] [int] NULL,
[ScheduledJobId] [int] NULL,
[ErrorDate] [datetime] NOT NULL CONSTRAINT [DF__Scheduled__Error__232BAE88] DEFAULT ([dbo].[uFn_GetSyncLocalDate]()),
[CallForID] [bigint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [dbo].[ScheduledJobsErrors] ADD CONSTRAINT [PK__Schedule__3214EC0721436616] PRIMARY KEY CLUSTERED  ([Id]) ON [PRIMARY]
GO

IF object_id('ScheduledJobs_Log') IS NOT NULL
	DROP TABLE [ScheduledJobs_Log]	
CREATE TABLE [dbo].[ScheduledJobs_Log]
(
[ID] [bigint] NOT NULL IDENTITY(1, 1),
[ScheduledJobId] [bigint] NULL,
[RunOn] [datetime] NOT NULL,
[LastRunOn] [datetime] NULL,
[LastRunOK] [bit] NOT NULL,
[CallForID] [int] NOT NULL,
[Error] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[Remarks] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO



IF OBJECT_ID('PS_DeleteScheduledJob') IS NOT NULL
	DROP PROC PS_DeleteScheduledJob

GO
Crea PROCEDURE dbo.PS_DeleteScheduledJob--By Prakash Seervi
@CallForID INT
AS
BEGIN
	DECLARE @iTotCount INT,@id  BIGINT,@iLoopCount INT=1,@ConversationHandle UNIQUEIDENTIFIER
	SELECT ROW_NUMBER() OVER (ORDER BY ID) AS UniqueId,ID,ConversationHandle INTO #tSch FROM ScheduledJobs WHERE CallForID=@CallForID AND IsEnabled=1

	SET @iTotCount=@@ROWCOUNT
	WHILE @iLoopCount<=@iTotCount
	BEGIN
		SELECT @id=ID,@ConversationHandle=ConversationHandle FROM #tSch WHERE UniqueId=@iLoopCount

		UPDATE dbo.ScheduledJobs SET IsEnabled=0,Error='Deleted',DeleteTime=dbo.uFn_GetSyncLocalDate() WHERE ID=@id

		IF EXISTS(SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle = @ConversationHandle)
			END CONVERSATION @ConversationHandle

		SET @iLoopCount=@iLoopCount+1

	END	
	DROP TABLE #tSch
END
GO


IF OBJECT_ID('PS_AddScheduledJob') IS NOT NULL
	DROP PROC PS_AddScheduledJob

GO
CREATE  PROCEDURE dbo.PS_AddScheduledJob---By Prakash Seervi
(
	@ScheduledSql	NVARCHAR(MAX), 
	@FirstRunOn		DATETIME, 
	@IsRepeatable	BIT,
	@CallForID		INTEGER,
	@ScheduledJobId INT=0 OUTPUT,
	@SrNo			SMALLINT	
)
AS
	DECLARE  @TimeoutInSeconds INT, @ConversationHandle UNIQUEIDENTIFIER	,@TransCounter BIGINT
	SET @TransCounter = @@TRANCOUNT

	IF @TransCounter <=0
	BEGIN TRANSACTION

	BEGIN TRY


		-- add job to our table
		INSERT INTO ScheduledJobs(ScheduledSql, FirstRunOn, IsRepeatable, ConversationHandle,CallForID,SrNo,NxtRunOn)
		VALUES (@ScheduledSql, @FirstRunOn, @IsRepeatable, NULL,@CallForID,@SrNo,@FirstRunOn)
		SELECT @ScheduledJobId = SCOPE_IDENTITY()
		

		-- set the timeout. It's in seconds so we need the datediff
		SELECT @TimeoutInSeconds = DATEDIFF(s, GETDATE(), @FirstRunOn);
		-- begin a conversation for our scheduled job
		BEGIN DIALOG CONVERSATION @ConversationHandle
			FROM SERVICE   [//ScheduledJobService]
			TO SERVICE      '//ScheduledJobService', 
							'CURRENT DATABASE'
			ON CONTRACT     [//ScheduledJobContract]
			WITH 
			ENCRYPTION = OFF;

		-- start the conversation timer
		BEGIN CONVERSATION TIMER (@ConversationHandle)
		TIMEOUT = @TimeoutInSeconds;
		-- associate or scheduled job with the conversation via the Conversation Handle
		UPDATE	ScheduledJobs
		SET		ConversationHandle = @ConversationHandle, 
				IsEnabled = 1
		WHERE	ID = @ScheduledJobId 
		IF @TransCounter <=0
		BEGIN 
			COMMIT;
		END
	END TRY
	BEGIN CATCH
		IF @TransCounter <=0
		BEGIN 
			ROLLBACK;
		END
		INSERT INTO ScheduledJobsErrors (
				ErrorLine, ErrorNumber, ErrorMessage, 
				ErrorSeverity, ErrorState, ScheduledJobId)
		SELECT	ERROR_LINE(), ERROR_NUMBER(), 'PS_AddScheduledJob: ' + ERROR_MESSAGE(), 
				ERROR_SEVERITY(), ERROR_STATE(), @ScheduledJobId
	END CATCH



GO


IF OBJECT_ID('usp_RunScheduledJob') IS NOT NULL
	DROP PROC usp_RunScheduledJob

GO
CREATE PROCEDURE dbo.PS_RunScheduledJob --By Prakash Seervi
AS
BEGIN
				
				DECLARE @IsSkipService BIT=0--if Want to skip this job then use 

				DECLARE 
				@ConversationHandle UNIQUEIDENTIFIER,-- conversation_handle Value
				@ScheduledJobId INT,--PK Id of schedule Job  
				@LastRunOn DATETIME,--current time to update last run date
				@IsEnabled BIT,--Show Status of current Job  
				@LastRunOK BIT,--show current Job Run sucessfull of not
				@iTimeInterVal INT,--Calculated time for next run 
				@CallForID INTEGER,--Identify which Block of code need to execute 
				@sErrCode NVARCHAR(MAX),--IF error occured then set error message/code,
				@dNxtSchDate DATETIME,--Next Job run date Time
				@iTryCount INTEGER=1,--if want to retry any code if an error occured 
				@ScheduledSql NVARCHAR(MAX),--if want to execute any query from schedule job table
				@IsRepeatable BIT,--use to Identify Job is one time or need to reschedule 
				@IsLogMaintain BIT-- Need to maintain Log or not
				
				DECLARE @LocalDate DATETIME =  GETDATE()--current date nad time
				DECLARE @ErrorCodeLog NVARCHAR(max)=''
				
				SELECT	@LastRunOn = GETDATE(), @IsEnabled = 1, @LastRunOK = 0
				-- we don't need transactions since we don't want to put the job back in the queue if it fails
				BEGIN TRY
					DECLARE @message_type_name sysname;			
					-- receive only one message from the queue
					RECEIVE TOP(1) 
						    @ConversationHandle = conversation_handle,
						    @message_type_name = message_type_name
					FROM ScheduledJobQueue

					-- exit if no message or other type of message than DialgTimer 
					IF @@ROWCOUNT = 0 OR ISNULL(@message_type_name, '') != 'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer'
						RETURN;
											 			
					-- get a scheduled job that is enabled and is associated with our conversation handle.
					-- if a job fails we disable it by setting IsEnabled to 0
					SELECT	@ScheduledJobId = ID, @ScheduledSql = ScheduledSql, @IsRepeatable = IsRepeatable,@CallForID = CallForID
					FROM	ScheduledJobs 
					WHERE	ConversationHandle = @ConversationHandle AND IsEnabled = 1

					
			SELECT 
	     		@dNxtSchDate= CASE WHEN FrequencyType = 1 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
	     							THEN DATEADD(MINUTE, INTERVAL, @LocalDate) -----For time Interval Like Daily Run After 10 Min,15 Min.......
									WHEN FrequencyType = 4 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
	     							THEN DATEADD(SECOND, INTERVAL, @LocalDate) -----For time Interval Like Daily Run After 10 Sec,15 Sec.......
	     	  ELSE 
	     			CASE WHEN FrequencyType = 1 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 --------For Run after every 7 days,3 days...... 
	     							THEN CAST(DATEADD(day, m_AutoSBCall.[DAY], dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))  AS DATETIME) + cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
	     			 WHEN FrequencyType = 1--------For Daily Run 
	     			 				THEN CAST(DATEADD(day, 1, dbo.uFn_GetDateToString(@LocalDate,CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE ''END    AS DATETIME)
	     			 WHEN FrequencyType = 2 AND ISNULL(m_AutoSBCall.[DAY],-1) = -1 
							THEN CAST(DATEADD(wk, 1,dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
					 WHEN FrequencyType = 2----------For Weekly Run 
	     			 				THEN CAST(DATEADD(wk, DATEDIFF(wk, 0, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)), m_AutoSBCall.[DAY] - 1) AS DATETIME) + Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
	     			 WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], 0) = 31 ----------For Last date of Month
	     			 				THEN CAST(DATEADD(dd, -( DAY(@LocalDate) ), DATEADD(mm, 1, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))) AS DATETIME) + Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
	     			 WHEN FrequencyType = 3 AND m_AutoSBCall.[DAY] <> -1---------For Monthly Run But on specific date Like Run On Every Month on 01,05 date 
	     			 				THEN CAST(DATEADD(month, ( ( YEAR(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) - 1900 )* 12 )+ MONTH(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)),m_AutoSBCall.[DAY] - 1)  AS DATETIME)+ Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
	     			 WHEN FrequencyType = 3 ---------------------------------------------------For Monthly Run
	     							THEN DATEADD(mm, 1,dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) END   
									
         END ,@DaysNotRunOn=isnull(DaysNotRunOn,'') 
	     FROM     dbo.JobSchedule AS m_AutoSBCall WHERE CallForID = @CallForID AND IsActive=1 
		
		 UPDATE dbo.ScheduledJobs SET SPID =@@SPID,LastRunOK=0 where CallForID = @CallForID and IsEnabled = 1
		  

		 				-- end the conversation if it's non repeatable
					IF @IsRepeatable = 0
					BEGIN			
						END CONVERSATION @ConversationHandle						
					END
					ELSE
					BEGIN 
						-- reset the timer to fire again in one day
						SELECT @iTimeInterVal=CASE WHEN FrequencyType =4 AND NULLIF(Interval,0)<>0 
							THEN  Interval ELSE ISNULL(NULLIF(NULLIF(Interval,0)*60,-1),DATEDIFF(s, GETDATE(), @dNxtSchDate)) end 
						FROM dbo.JobSchedule WHERE CallForID = @CallForID
						BEGIN CONVERSATION TIMER (@ConversationHandle)
							TIMEOUT = isnull(@iTimeInterVal,86400); 
					
					END			


					--run our job
					IF LEN(ISNULL(@ScheduledSql,''))>0
						EXEC sys.sp_executesql @query=@ScheduledSql,
						@params = N'@ErrorCode	NVARCHAR(MAX) OUTPUT',
						@ErrorCode = @sErrCode OUTPUT

					ELSE IF  @IsSkipService=0
					BEGIN 
							IF  @CallForID=1 
							BEGIN
							PRINT 'Write your script need to execute in this job'
								--EXEC <Procudre_name>
								
							END
							IF  @CallForID=2 
							BEGIN
								print	'Update <Query>'
							END
							IF @CallForID=3
							BEGIN
								PRINT 'Delete <Query>'
							END 
							IF @CallForID=4
								BEGIN
									PRINT 'Inert <Query>'
								END
							IF @CallForID=5 --Re-Try Block
							BEGIN
	
								RETRY: 
											PRINT '<Query>'
											SET @sErrCode=''
								IF @sErrCode<>'0' AND @iTryCount<4 
								BEGIN	
									SET @iTryCount=@iTryCount+1
									GOTO RETRY -- Go to Label RETRY
								END

								IF @iTryCount>1		
									SET @sErrCode= ' Try Count :' + CAST(@iTryCount AS Nvarchar(18)) + ' ' + @sErrCode
									
							END
					
                       		SELECT @LastRunOK = 1
					END
				END TRY
				BEGIN CATCH		
					--SELECT @IsEnabled = 0

					UPDATE ScheduledJobs SET SrNo = 0 WHERE CallForID = @CallForID AND SrNo = 1 AND IsEnabled = 1

					INSERT INTO ScheduledJobsErrors (
							ErrorLine, ErrorNumber, ErrorMessage, 
							ErrorSeverity, ErrorState, ScheduledJobId,CallForID)
					SELECT	ERROR_LINE(), ERROR_NUMBER(), 'vsp_RunScheduledJob: ' + ERROR_MESSAGE(), 
							ERROR_SEVERITY(), ERROR_STATE(), @ScheduledJobId,@CallForID
					
					
					SET @ErrorCodeLog= CAST(ERROR_LINE() AS NVARCHAR(max)) +' AS  ERROR_LINE| '+CAST(ERROR_NUMBER() AS NVARCHAR(max))+' as ERROR_NUMBER| '+ ERROR_MESSAGE() +' AS ERROR_MESSAGE|' +
										CAST( ERROR_SEVERITY() AS NVARCHAR(max)) + ' as ERROR_SEVERITY| ' +CAST( ERROR_STATE() AS NVARCHAR(max)) +' as ERROR_STATE| '+CAST( @ScheduledJobId AS NVARCHAR(max)) +' as ScheduledJobId | '+CAST( @CallForID AS NVARCHAR(max)) +' as CallForID '
					
					SET @LastRunOK=0

					
					-- if an error happens end our conversation if it exists
					IF @ConversationHandle != NULL		
					BEGIN
						IF EXISTS (SELECT * FROM sys.conversation_endpoints WHERE conversation_handle = @ConversationHandle)
							END CONVERSATION @ConversationHandle
					END

				END CATCH;

		 SELECT 
	     		@dNxtSchDate= CASE WHEN FrequencyType = 1 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
	     							THEN DATEADD(MINUTE, INTERVAL, @LocalDate) -----For time Interval Like Daily Run After 10 Min,15 Min.......
									WHEN FrequencyType = 4 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
	     							THEN DATEADD(SECOND, INTERVAL, @LocalDate) -----For time Interval Like Daily Run After 10 Sec,15 Sec.......
	     	  ELSE 
	     		CASE WHEN FrequencyType = 1 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 
	     							THEN Cast(DATEADD(day, m_AutoSBCall.[DAY], dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)--------For Run after every 7 days,3 days...... 
	     			 WHEN FrequencyType = 1 
	     			 				THEN Cast(DATEADD(day, 1, dbo.uFn_GetDateToString(@LocalDate,CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE ''END    AS DATETIME)--------For Daily Run
	     			 WHEN FrequencyType = 2 AND ISNULL(m_AutoSBCall.[DAY],-1) = -1 
							THEN Cast(DATEADD(wk, 1,dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
					 WHEN FrequencyType = 2 
	     			 				THEN Cast(DATEADD(wk, DATEDIFF(wk, 0, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)), m_AutoSBCall.[DAY] - 1)  AS DATETIME)+ Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)----------For Weekly Run
	     			 WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], 0) = 31 
	     			 				THEN Cast(DATEADD(dd, -( DAY(@LocalDate) ), DATEADD(mm, 1, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)))  AS DATETIME)+ cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)----------For Last date of Month
	     			 WHEN FrequencyType = 3 AND m_AutoSBCall.[DAY] <> -1 
	     			 				THEN Cast(DATEADD(month, ( ( YEAR(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) - 1900 )* 12 )+ MONTH(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)),m_AutoSBCall.[DAY] - 1)  AS DATETIME)+ Cast(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END   AS DATETIME)---------For Monthly Run But on specific date Like Run On Every Month on 01,05 date
	     			 WHEN FrequencyType = 3 
	     							THEN DATEADD(mm, 1,dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) END   ---------------------------------------------------For Monthly Run
         END 
	     FROM     dbo.JobSchedule AS m_AutoSBCall WHERE CallForID = @CallForID AND IsActive=1 
		
				SELECT	@LastRunOn = GETDATE()
				-- update the job status
				UPDATE	ScheduledJobs
				SET		LastRunOn = @LastRunOn,
						IsEnabled = CASE WHEN @IsRepeatable = 0 then 0 ELSE @IsEnabled END ,
						LastRunOK = @LastRunOK,
						NxtRunOn=@dNxtSchDate,
						Error = @sErrCode
				WHERE	ID = @ScheduledJobId

				SELECT @IsLogMaintain=IsLogMaintain FROM dbo.JobSchedule WHERE CallForID = @CallForID
			if isnull(@IsLogMaintain,0)=1
				begin 	
				INSERT INTO dbo.ScheduledJobs_Log
				(
				    ScheduledJobId,RunOn,LastRunOn,LastRunOK,CallForID,Error,Remarks
				)
				VALUES
				(   @ScheduledJobId,         -- ScheduledJobId - bigint
				    @LocalDate, -- RunOn - datetime
				    GETDATE(), -- LastRunOn - datetime
				    @LastRunOK,      -- LastRunOK - bit
				    @CallForID,         -- CallForID - int
					@ErrorCodeLog,       -- Error - nvarchar(max)
				    @sErrCode        -- Remarks - nvarchar(500)
				)
				end 		
END
GO



IF OBJECT_ID('PS_MaintainScheduledJob') IS NOT NULL
	DROP PROC PS_MaintainScheduledJob

GO
Create PROCEDURE dbo.PS_MaintainScheduledJob--By Prakash Seervi 
@IsCallByService BIT=1,
@CallForID BIGINT=0,
@IsImmediateInitiate BIT=0,
@OptionalStatrTime DATETIME='',
@User AS NVARCHAR(100)
AS	
BEGIN

	BEGIN TRY 
		DECLARE @DatabaseName AS NVARCHAR(30)= DB_NAME() 
		DECLARE @sSql AS NVARCHAR(1000)='',@ISPrincplError BIT = 0
		

		-- This Block Handle if service broker disabled or Service broker Not running or if DB restore and need to start serive broker
IF EXISTS (SELECT 1 FROM sys.database_principals  WHERE name ='dbo' AND type_desc <>'SQL_USER') OR 
	NOT EXISTS(SELECT 1 FROM sys.database_principals  WHERE name ='dbo' AND 
sid IN (SELECT SD.sid FROM master..sysdatabases AS SD INNER JOIN master..syslogins SL ON SD.SID = SL.SID WHERE SD.name = DB_NAME() )
)
		SET @ISPrincplError=1
	IF EXISTS(SELECT * FROM sys.databases WHERE name = DB_NAME() AND (is_broker_enabled=0 )) OR @ISPrincplError=1
	   SET @sSql='ALTER DATABASE ['+ @DatabaseName +'] SET NEW_BROKER WITH ROLLBACK IMMEDIATE;'	
	IF EXISTS(SELECT * FROM sys.databases WHERE name = DB_NAME() AND (is_trustworthy_on=0)) OR @ISPrincplError=1
	   SET @sSql +='ALTER DATABASE ['+ @DatabaseName +'] SET TRUSTWORTHY ON ;'	
	IF NOT EXISTS(SELECT * FROM master..sysdatabases AS SD INNER JOIN master..syslogins SL ON SD.SID = SL.SID WHERE SD.name = DB_NAME() AND sl.name =@User)  OR @ISPrincplError=1
	   SET @sSql +='ALTER AUTHORIZATION ON DATABASE::['+ @DatabaseName +'] TO ['+@User+']'	
	EXEC (@sSql)

	IF @ISPrincplError=1---this block reschedule All Jobs; if service broker restart then need to reschedule all jobs
	BEGIN
			SELECT * INTO #TempJobSchedule  FROM dbo.JobSchedule WHERE IsActive = 1
			UPDATE JobSchedule SET IsActive = 0
			
			UPDATE JobSchedule SET IsActive=1 FROM #TempJobSchedule
			WHERE #TempJobSchedule.CallForID=JobSchedule.CallForID
	END 

	ELSE IF EXISTS (SELECT 1 FROM sys.dm_broker_queue_monitors WHERE database_id = DB_ID() and state ='NOTIFIED')
	BEGIN---If queue is inactive this it will reactivate
			DECLARE @QueueTable NVARCHAR(max)=''
			SELECT @QueueTable=name FROM sys.dm_broker_queue_monitors 
					INNER JOIN sys.service_queues ON queue_id=object_id
					WHERE database_id = DB_ID() and state ='NOTIFIED' 
					AND name ='ScheduledJobQueue'

			SET @sSql=''
			IF ISNULL(@QueueTable,'')<>''
				SET @sSql='
				ALTER QUEUE '+@QueueTable+'  WITH STATUS = OFF , RETENTION = OFF , ACTIVATION ( STATUS = OFF )
				Go

				ALTER QUEUE '+@QueueTable+' WITH STATUS = ON , RETENTION = OFF , ACTIVATION ( STATUS = ON )
				Go
				'

				EXEC (@sSql);
	END 
	END TRY
	BEGIN CATCH
	END CATCH

	DECLARE @SchDate DATETIME,@sStartTime AS VARCHAR(10),@JobId BIGINT,@ConversationHandle UNIQUEIDENTIFIER,@dLastRunDt DATETIME,@dFirstRunDt DATETIME,@IsEnabled BIT=0
	
	SELECT	@ConversationHandle = ConversationHandle,@dLastRunDt=LastRunOn,@IsEnabled=IsEnabled FROM ScheduledJobs 
	WHERE CallForID=12 ORDER BY ID
	
	

DECLARE @rowCunt BIGINT = 0 ,@LocalDate DATETIME =  dbo.uFn_GetSyncLocalDate()

SELECT m_AutoSBCall.CallForID as ttCallForID,m_AutoSBCall.IsRepeatable AS ttIsRepeatable, ROW_NUMBER()OVER (ORDER BY (SELECT 1 ) ) AS Rno,
    CASE WHEN FrequencyType = 1 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
								THEN DATEADD(MINUTE, INTERVAL, @LocalDate) -----For time Interval Like Daily Run After 10 Min,15 Min.......
    ELSE 
	CASE WHEN FrequencyType = 1 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 
								THEN CAST(DATEADD(day, m_AutoSBCall.[DAY], dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)--------For Run after every 7 days,3 days...... 
	     WHEN FrequencyType = 1 
								THEN CAST(DATEADD(day, 1, dbo.uFn_GetDateToString(@LocalDate,CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE ''END    AS DATETIME)--------For Daily Run
         WHEN FrequencyType = 2 AND ISNULL(m_AutoSBCall.[DAY],-1) = -1 
								THEN CAST(DATEADD(wk,  1, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)----------For Weekly Run
    	 WHEN FrequencyType = 2 
								THEN CAST(DATEADD(wk, DATEDIFF(wk, 0, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)), m_AutoSBCall.[DAY] - 1) AS DATETIME) + CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)----------For Weekly Run
         WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], 0) = 31 
								THEN CAST(DATEADD(dd, -( DAY(@LocalDate) ), DATEADD(mm, 1, dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)))  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)----------For Last date of Month
         WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 
								THEN CAST(DATEADD(month, ( ( YEAR(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) - 1900 )* 12 )+ MONTH(dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)),m_AutoSBCall.[DAY] - 1)  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME) ---------For Monthly Run But on specific date Like Run On Every Month on 01,05 date
         WHEN FrequencyType = 3 
								THEN DATEADD(mm, 1,dbo.uFn_GetDateToString(@LocalDate, CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) END   ---------------------------------------------------For Monthly Run
    END AS NextDate ,---Next Run Date Acc To Current Date
    CASE WHEN FrequencyType = 1 AND ISNULL(NULLIF(INTERVAL, -1), '') <> '' 
								THEN DATEADD(MINUTE, INTERVAL ,ISNULL(LastRunOn, FirstRunOn))
    ELSE 
	CASE WHEN FrequencyType = 1 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 
								THEN CAST(DATEADD(day, m_AutoSBCall.[DAY] ,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END)) AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
         WHEN FrequencyType = 1 
								THEN CAST(DATEADD(day, 1,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
         WHEN FrequencyType = 2 AND ISNULL(m_AutoSBCall.[DAY],-1) = -1 
								THEN CAST(DATEADD(wk, 0,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END))  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
		 WHEN FrequencyType = 2 
								THEN CAST(DATEADD(wk,DATEDIFF(wk, 0,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END)),m_AutoSBCall.[DAY])  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
         WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], 0) = 31 
								THEN CAST(DATEADD(dd,-( DAY(ISNULL(LastRunOn, FirstRunOn)) ),DATEADD(mm, 1,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END)))  AS DATETIME)+ CAST(CASE WHEN ISNULL(time, '') <> '' THEN Time ELSE '' END  AS DATETIME)
         WHEN FrequencyType = 3 AND ISNULL(m_AutoSBCall.[DAY], -1) <> -1 
								THEN CAST(DATEADD(month,( ( YEAR(dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END)) - 1900 )* 12 )+ MONTH(dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn), CASE WHEN ISNULL(time,'') <> '' THEN 0 ELSE 1 END)),m_AutoSBCall.[DAY]-1) AS DATETIME) + CAST(CASE WHEN ISNULL(time, '') <> '' THEN TIME ELSE '' END  AS DATETIME)
         WHEN FrequencyType = 3 
								THEN DATEADD(mm, 1,dbo.uFn_GetDateToString(ISNULL(LastRunOn,FirstRunOn),CASE WHEN ISNULL(time, '') <> '' THEN 0 ELSE 1 END)) END
    END AS TillNextRunON ,--Must Run Next Date Acc To Last Run Date	
	ScheduledJobs.* INTO #TempScheduled
FROM       dbo.JobSchedule AS m_AutoSBCall
        LEFT JOIN   dbo.ScheduledJobs ON m_AutoSBCall.CallForID = dbo.ScheduledJobs.CallForID AND IsEnabled = 1
		LEFT JOIN  sys.dm_broker_activated_tasks ON dm_broker_activated_tasks.spid =ISNULL( ScheduledJobs.SPID,0) AND database_id=DB_ID()
WHERE    IsActive=1 AND m_AutoSBCall.CallForID NOT IN (7) AND dm_broker_activated_tasks.spid IS NULL
SET @rowCunt = @@ROWCOUNT
       	
	IF @IsImmediateInitiate=1 AND ISNULL(@CallForID,0)<>0
	BEGIN 
			     DECLARE @Currentdate DATETIME = ''
			     SELECT @Currentdate = CASE WHEN ISNULL(@OptionalStatrTime,'')<> '' THEN @OptionalStatrTime ELSE DATEADD(MINUTE, 5, dbo.uFn_GetSyncLocalDate()) END 
			     
			     EXEC dbo.PS_DeleteScheduledJob @CallForID = @CallForID
			 -- int
			     EXEC dbo.PS_AddScheduledJob @ScheduledSql = N'', -- nvarchar(max)
			        @FirstRunOn = @Currentdate, -- datetime
			        @IsRepeatable = 1, -- bit
			        @CallForID = @CallForID, -- int
			        @SrNo = 0 -- smallint 
			

	END 
	ELSE
	BEGIN 	
	 declare @NextSchedulTime DATETIME='',@Isrepeatable BIT=0,@ScheduledSql NVARCHAR(max)=''
		--SELECT * FROM #TempScheduled
	--SELECT * FROM #TempScheduled WHERE NextDate>TillNextRunON
		--if EXISTS (SELECT 1 FROM #TempScheduled WHERE NextDate>ISNULL(TillNextRunON,DATEADD(MINUTE, -5, NextDate)))
		if EXISTS (SELECT 1 FROM #TempScheduled WHERE ISNULL(DATEADD(HOUR,1,NxtRunOn),DATEADD(MINUTE, -10,dbo.uFn_GetSyncLocalDate()))< dbo.uFn_GetSyncLocalDate())
		BEGIN 
			while  @rowCunt >0			
			begin		
			    SET @CallForID=0
				SELECT @CallForID=ISNULL(CallForID,ttCallForID),@ScheduledSql=ScheduledSql,@Isrepeatable=ISNULL(ttIsRepeatable,0),@NextSchedulTime=NextDate 
				FROM #TempScheduled WHERE Rno = @rowCunt AND   ISNULL(DATEADD(HOUR,1,NxtRunOn),DATEADD(MINUTE, -10,dbo.uFn_GetSyncLocalDate()))<  dbo.uFn_GetSyncLocalDate()
			  -- SELECT @CallForID,@NextSchedulTime
			   IF ISNULL(@CallForID,0)<>0
			   BEGIN
			    EXEC dbo.PS_DeleteScheduledJob @CallForID = @CallForID -- int
				EXEC dbo.PS_AddScheduledJob @ScheduledSql = N'', -- nvarchar(max)
				    @FirstRunOn = @NextSchedulTime, -- datetime
				    @IsRepeatable = @Isrepeatable, -- bit
				    @CallForID = @CallForID, -- int				   
				    @SrNo = 0 -- smallint
			    END 			
				SET @rowCunt= @rowCunt -1 			
			END 
		END 
			

		IF EXISTS (SELECT COUNT(1),CallForID FROM dbo.ScheduledJobs WHERE IsEnabled = 1 AND CallForID NOT IN (-1,7) GROUP BY CallForID HAVING COUNT(1) > 1 )
		BEGIN 
				SELECT  ROW_NUMBER()OVER (ORDER BY (SELECT 1 ) ) AS Rno,COUNT(1) AS cnt,CallForID,MAX(ISNULL(LastRunOn,FirstRunOn)) AS NextDate
				,ScheduledSql,IsRepeatable 
				INTO #tempShcdul
				 FROM dbo.ScheduledJobs WHERE IsEnabled = 1  AND CallForID NOT IN (-1,7) GROUP BY CallForID ,ScheduledSql,IsRepeatable HAVING COUNT(1) > 1 
				SET @rowCunt = @@ROWCOUNT		
	
	
		WHILE  @rowCunt >0			
			begin		
			    SET @CallForID=0			 
				SELECT @CallForID = ISNULL(CallForID, 0),
							       @ScheduledSql = ScheduledSql,
							       @Isrepeatable = IsRepeatable,
							       @NextSchedulTime = NextDate
							FROM #tempShcdul
							WHERE Rno = @rowCunt					

			   IF ISNULL(@CallForID,0)<>0
			   BEGIN
			    EXEC dbo.PS_DeleteScheduledJob @CallForID = @CallForID -- int
				EXEC dbo.PS_AddScheduledJob @ScheduledSql = N'', -- nvarchar(max)
				    @FirstRunOn = @NextSchedulTime, -- datetime
				    @IsRepeatable = @Isrepeatable, -- bit
				    @CallForID = @CallForID, -- int				   
				    @SrNo = 0 -- smallint
			    END 			
				SET @rowCunt= @rowCunt -1 			
			END 


		END 
	END 



--------------------------------------------------------------------------------------Yaha se pdhana haii----------------------------------------------------------------------

----------------------------

-----ye Query hr min se chlanii ho to. for eg. har 10 min se to interval me 10 dalna haii..!!
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=1,DAY=-1,Time=NULL,Interval=[minute dalna haii] WHERE CallForID = ??   
 
 
-----ye Query Daily chlanii chahiye particular time pe . for eg. daily 1 bje chlnii chahiye to time me 01:00 dalna haii ..!!
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=1,DAY=-1,Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??  
  
  -----ye Query particular day pe repert honii chahiye and particular time pe . for eg. har 2 din baad 1  bje chlnii chahiye to day me 2 aayega and time me 01:00 dalna haii ..!!
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=1,DAY=[Day dalna haii],Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   
 

----Ye query Week me jis din schedule kii haii us din agle week kii schedule krnii ho to kaam aayegii .. bss time update krna haii
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=2,DAY=-1,Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   

----Ye query Week me jis particular day jese monday ko  schedule krnii ho to day me 1 dalege and time update krna haii to ye query har monday ko chlegii ....
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=2,DAY=[day dlana haii],Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   



----Ye query month ke last day ko schedule krnii ho to kaam aayegii bss isme time update krna haii ....
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=3,DAY=31,Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   


----Ye query har month kii particular day ko schedule krnii ho to haam aayegii jese har mahinne 2 takri ko chlnii chahiye to day me 2 update krna haii and time update krna haii ....
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=3,DAY=[Day dalna haii],Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   


----Ye query  jis dine schedule kroge usdin har Next month kii ReSchedule krega ...!!
--UPDATE dbo.JobSchedule SET IsActive=1,FrequencyType=3,DAY=-1,Interval=NULL,TIME=[Time dalna haii] WHERE CallForID = ??   




----------------------------Case when ------- Fresh Entry Done And for Auto schedule 
--EXEC dbo.rsp_MaintainScheduledJob
------------------------------------------------------------------------------------------


--------------------------------------Service Already Running But Time Change and Need to reschedule auto acc. to new time ----------------------
--EXEC dbo.PS_DeleteScheduledJob @CallForID = 62-- int
--EXEC dbo.PS_MaintainScheduledJob
----------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------Service Schedule But Run now ------ not on given time ------for first time ------
 --    DECLARE @Currentdate DATETIME = '' ,@CallForID BIGINT=62
 --    SELECT @Currentdate = DATEADD(SECOND, 10, dbo.uFn_GetSyncLocalDate())
     
 --    EXEC dbo.PS_DeleteScheduledJob @CallForID = @CallForID
 ---- int
     
 --    EXEC dbo.PS_AddScheduledJob @ScheduledSql = N'', -- nvarchar(max)
 --       @FirstRunOn = @Currentdate, -- datetime
 --       @IsRepeatable = 1, -- bit
 --       @CallForID = @CallForID, -- int
 --       @SrNo = 0 -- smallint 

 ------------------------------------------------------------------------------------------------------------------------



 -------------------------------------------------------only for one time ------------------------------------------------------
 
 --    DECLARE @Currentdate DATETIME = '' ,@CallForID BIGINT=62
 --    SELECT @Currentdate = DATEADD(SECOND, 10, dbo.uFn_GetSyncLocalDate())
     
 --    EXEC dbo.PS_DeleteScheduledJob @CallForID = @CallForID
 ---- int
     
 --    EXEC dbo.PS_AddScheduledJob @ScheduledSql = N'', -- nvarchar(max)
 --       @FirstRunOn = @Currentdate, -- datetime
 --       @IsRepeatable = 0, -- bit
 --       @CallForID = @CallForID, -- int
 --       @SrNo = 0 -- smallint 

-----------------------------------------------------------------------------------------------------------------

END
GO


IF EXISTS(SELECT * FROM sys.services WHERE NAME = N'//ScheduledJobService')
	DROP SERVICE [//ScheduledJobService]

IF EXISTS(SELECT * FROM sys.service_queues WHERE NAME = N'ScheduledJobQueue')
	DROP QUEUE ScheduledJobQueue

IF EXISTS(SELECT * FROM sys.service_contracts  WHERE NAME = N'//ScheduledJobContract')
	DROP CONTRACT [//ScheduledJobContract]

GO
CREATE CONTRACT [//ScheduledJobContract]
	([http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer] SENT BY INITIATOR)

CREATE QUEUE ScheduledJobQueue 
	WITH STATUS = ON, 
	ACTIVATION (	
		PROCEDURE_NAME = usp_RunScheduledJob,
		MAX_QUEUE_READERS = 20, -- we expect max 20 jobs to start simultaneously
		EXECUTE AS 'dbo' );

CREATE SERVICE [//ScheduledJobService] 
	AUTHORIZATION dbo
	ON QUEUE ScheduledJobQueue ([//ScheduledJobContract])

--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- T E S T
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
GO
DECLARE @ScheduledSql nvarchar(max), @RunOn datetime, @IsRepeatable BIT
SELECT	@ScheduledSql = N'DECLARE @backupTime DATETIME, @backupFile NVARCHAR(512); 
						  SELECT @backupTime = GETDATE(), 
						         @backupFile = ''C:\TestScheduledJobs_'' + 
						                       replace(replace(CONVERT(NVARCHAR(25), @backupTime, 120), '' '', ''_''), '':'', ''_'') + 
						                       N''.bak''; 
						  BACKUP DATABASE TestScheduledJobs TO DISK = @backupFile;',
		@RunOn = dateadd(s, 30, getdate()), 
		@IsRepeatable = 0

EXEC PS_AddScheduledJob @ScheduledSql, @RunOn, @IsRepeatable
GO

DECLARE @ScheduledSql nvarchar(max), @RunOn datetime, @IsRepeatable BIT
SELECT	@ScheduledSql = N'select 1, where 1=1',
		@RunOn = dateadd(s, 30, getdate()), 
		@IsRepeatable = 1

EXEC PS_AddScheduledJob @ScheduledSql, @RunOn, @IsRepeatable
GO

DECLARE @ScheduledSql nvarchar(max), @RunOn datetime, @IsRepeatable BIT
SELECT	@ScheduledSql = N'EXEC sp_updatestats;', 
		@RunOn = dateadd(s, 30, getdate()), 
		@IsRepeatable = 0

EXEC PS_AddScheduledJob @ScheduledSql, @RunOn, @IsRepeatable
GO

--EXEC PS_DeleteScheduledJob 1
--EXEC PS_DeleteScheduledJob 1
--EXEC PS_DeleteScheduledJob 3
GO

-- show the currently active conversations. 
-- Look at dialog_timer column to see when will the job be run next
SELECT * FROM sys.conversation_endpoints
-- shows the number of currently executing activation procedures
SELECT * FROM sys.dm_broker_activated_tasks
-- see how many unreceived messages are still in the queue. 
-- should be 0 when no jobs are running
SELECT * FROM ScheduledJobQueue with (nolock)
-- view our scheduled jobs' statuses
SELECT * FROM ScheduledJobs  with (nolock)
-- view any scheduled jobs errors that might have happend
SELECT * FROM ScheduledJobsErrors  with (nolock)