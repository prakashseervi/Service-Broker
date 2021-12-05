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
