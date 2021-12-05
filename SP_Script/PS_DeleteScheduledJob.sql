ALTER PROCEDURE dbo.PS_DeleteScheduledJob--By Prakash Seervi
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
