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
