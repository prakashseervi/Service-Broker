CREATE TABLE [dbo].[JobSchedule]
(
[CallForID] [int] NOT NULL,
[CallForService] [nvarchar] (200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[LastCallOn] [datetime] NULL,
[IsActive] [bit] NULL,
[IsVisible] [bit] NULL,
[FrequencyType] [int] NOT NULL,
[DAY] [int] NULL,
[Time] [time] NULL,
[Interval] [int] NULL,
[IsRepeatable] [bit] NULL,
[Retry] [int] NULL,
[IsLogMaintain] [bit] NULL,
[DaysNotRunOn] [nvarchar] (500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[IsStopForOneTime] [bit] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].JobSchedule ADD CONSTRAINT [CK_JobSchedule_Frequ__25518C17] CHECK (([FrequencyType]=(4) OR [FrequencyType]=(3) OR [FrequencyType]=(2) OR [FrequencyType]=(1)))
GO
ALTER TABLE [dbo].JobSchedule  ADD CONSTRAINT [PK_JobSchedule_New] PRIMARY KEY CLUSTERED  ([CallForID]) WITH (FILLFACTOR=80) ON [PRIMARY]
GO
