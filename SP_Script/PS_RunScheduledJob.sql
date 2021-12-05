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
	     FROM     dbo.m_AutoSBCall_New AS m_AutoSBCall WHERE CallForID = @CallForID AND IsActive=1 
		
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
						FROM dbo.m_AutoSBCall_NEW WHERE CallForID = @CallForID
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
	     FROM     dbo.m_AutoSBCall_New AS m_AutoSBCall WHERE CallForID = @CallForID AND IsActive=1 
		
				SELECT	@LastRunOn = GETDATE()
				-- update the job status
				UPDATE	ScheduledJobs
				SET		LastRunOn = @LastRunOn,
						IsEnabled = CASE WHEN @IsRepeatable = 0 then 0 ELSE @IsEnabled END ,
						LastRunOK = @LastRunOK,
						NxtRunOn=@dNxtSchDate,
						Error = @sErrCode
				WHERE	ID = @ScheduledJobId

				SELECT @IsLogMaintain=IsLogMaintain FROM dbo.m_AutoSBCall_New WHERE CallForID = @CallForID
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
