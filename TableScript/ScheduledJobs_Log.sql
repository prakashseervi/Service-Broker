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

