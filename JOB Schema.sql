USE [STG]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobParamsLookup](
[JobParamId] [int] IDENTITY(1,1) NOT NULL, [JobParamName] [varchar](100) NULL, [JobParamType] [varchar](500) NULL,
CONSTRAINT [PK_JobParamsLookup] PRIMARY KEY CLUSTERED
(
[JobParamId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobExecutionContextVariable](
[JobExecutionContextVariableId] [int] IDENTITY(1,1) NOT NULL, [JobRequestId] [int] NULL,
[JobParamId] [int] NULL,
[VariableValue] [varchar](max) NULL,
[Datetimestamp] [datetime] NULL,
CONSTRAINT [PK_JobExecutionContextVariable] PRIMARY KEY CLUSTERED
(
[JobExecutionContextVariableId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [JOB].[vwJobExecutionContextVariable] as
select cv.JobRequestId, para.JobParamName, cv.VariableValue, cv.Datetimestamp, cv.JobExecutionContextVariableId
from JOB.JobExecutionContextVariable cv

inner join JOB.JobParamsLookup para on cv.JobParamId = para.JobParamId
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobStatuses](
[Id] [int] IDENTITY(1,1) NOT NULL,
[Name] [varchar](100) NULL,
[Description] [varchar](500) NULL,
CONSTRAINT [PK_JobStatuses] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[Jobs](
[Id] [int] IDENTITY(1,1) NOT NULL,
[Name] [varchar](100) NOT NULL,
[IsActive] [tinyint] NULL,
CONSTRAINT [PK_Jobs] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobRequest](
[Id] [int] IDENTITY(1,1) NOT NULL,
[JobId] [int] NULL,
[JobStatusID] [int] NULL,
[ScheduleTime] [datetime] NULL,
CONSTRAINT [PK_JobRequest] PRIMARY KEY CLUSTERED
(

[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [JOB].[vwJobRequests]
As
Select
jr.Id as JobRequestId,
j.Id as JobId,
j.Name JobName,
js.Name as JobStatus, jr.ScheduleTime as ScheduleTime
from JOB.JobRequest jr
inner join JOB.Jobs j on jr.JobId = j.Id
inner join JOB.JobStatuses js on jr.JobStatusID = js.Id
and j.IsActive = 1
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[Schedules](
[ScheduleId] [int] IDENTITY(1,1) NOT NULL, [Name] [varchar](200) NOT NULL,
[Description] [varchar](200) NOT NULL, [CronScheduleExpression] [varchar](100) NULL, [IsActive] [bit] NOT NULL,
CONSTRAINT [PK_Schedules] PRIMARY KEY CLUSTERED
(
[ScheduleId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY], UNIQUE NONCLUSTERED
(
[Name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobScheduleMap](
[Id] [int] IDENTITY(1,1) NOT NULL,
[JobId] [int] NOT NULL,
[ScheduleId] [int] NOT NULL,
[IsActive] [bit] NOT NULL,
CONSTRAINT [PK_JobScheduleMap] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE View [JOB].[vwJobSchedules]
as
SELECT
jsm.Id as JobScheduleId,
j.Id as JobId,
j.Name as JobName,
s.ScheduleId as ScheduleId,
s.Name as ScheduleName, s.CronScheduleExpression as ScheduleExpression, jsm.IsActive as IsJobScheduleActive
FROM JOB.Jobs j
INNER JOIN JOB.JobScheduleMap jsm on j.Id = jsm.JobId
INNER JOIN JOB.Schedules s on jsm.ScheduleId = s.ScheduleId where s.IsActive =1
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[Tasks](
[Id] [int] IDENTITY(1,1) NOT NULL,
[Name] [varchar](100) NULL,
[Description] [varchar](2000) NULL,
[ClassHandler] [varchar](500) NULL,
[DbProcHandler] [varchar](8000) NULL,

CONSTRAINT [PK_Tasks] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobWorkflowExecutionTracker](
[id] [int] IDENTITY(1,1) NOT NULL,
[JobRequestId] [int] NOT NULL,
[TaskId] [int] NOT NULL,
[SequenceNum] [int] NOT NULL,
[ExecutionStatusId] [int] NOT NULL,
[CreatedDatetimestamp] [datetime] NOT NULL,
[UpdatedDatetimestamp] [datetime] NULL,
CONSTRAINT [PK_JobWorkflowExecutionTracker] PRIMARY KEY CLUSTERED
(
[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [JOB].[vwJobWorkflowExecutionTracker] as
select wt.JobRequestId,
j.Name as JobName,
tsk.Name as TaskName,
wt.SequenceNum,
js.Name as ExecutionStatus,
wt.id as JobWorkflowExecutionTrackerId, tsk.ID as TaskID, wt.CreatedDatetimestamp, wt.UpdatedDatetimestamp
from JOB.JobWorkflowExecutionTracker wt
inner join JOB.JobRequest jr on wt.JobRequestId = jr.Id inner join JOB.Jobs j on jr.JobId = j.Id
inner join JOB.Tasks tsk on wt.TaskId = tsk.Id

inner join JOB.JobStatuses js on wt.ExecutionStatusId = js.Id
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[JobWorkflow](
[Id] [int] IDENTITY(1,1) NOT NULL,
[JobId] [int] NOT NULL,
[TaskId] [int] NOT NULL,
[SequenceNum] [int] NOT NULL,
CONSTRAINT [PK_JobWorkflow] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create View [JOB].[vwJobWorkflow] as
select
j.Name as Jobname
,t.Name as TaskName ,w.SequenceNum ,w.Id as WorkflowId ,j.Id as JobId ,t.Id as TaskID
from JOB.Jobs j inner join JOB.JobWorkflow w on
j.Id = w.JobId inner join JOB.Tasks t on w.TaskId = t.Id
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[EmailRequests](

[Id] [int] IDENTITY(1,1) NOT NULL,
[JobRequestId] [int] NOT NULL,
[ToList] [varchar](8000) NULL,
[Importance] [varchar](25) NULL,
[Subject] [varchar](1000) NULL,
[Body] [varchar](max) NULL,
[BodyFormat] [varchar](20) NULL,
[CcList] [varchar](8000) NULL,
[BccList] [varchar](8000) NULL,
[AttachmentsList] [varchar](4000) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[ExecuteSqlRequests](
[Id] [int] IDENTITY(1,1) NOT NULL,
[JobRequestId] [int] NOT NULL,
[SqlCommandText] [varchar](8000) NOT NULL,
[SuccessEmailAlert] [bit] NOT NULL,
[FailureEmailAlert] [bit] NOT NULL,
[EmailList] [varchar](8000) NULL
) ON [PRIMARY]
GO /*********************************************************************************** ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [JOB].[FTPProfiles](
[FTPProfileId] [int] IDENTITY(1,1) NOT NULL,
[ProfileName] [varchar](50) NULL,
[UserName] [varchar](50) NULL,
[TargetServer] [varchar](50) NULL,
[TargetPath] [varchar](500) NULL,
[SshHostKeyFingerprint] [varchar](200) NULL,
PRIMARY KEY CLUSTERED
(
[FTPProfileId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [JOB].[ExecuteSqlRequests] ADD DEFAULT ((0)) FOR [SuccessEmailAlert] GO
ALTER TABLE [JOB].[ExecuteSqlRequests] ADD DEFAULT ((0)) FOR [FailureEmailAlert]

GO
ALTER TABLE [JOB].[Schedules] ADD DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [JOB].[EmailRequests] WITH CHECK ADD CONSTRAINT [FK_JobRequestID_EmailRequests] FOREIGN KEY([JobRequestId])
REFERENCES [JOB].[JobRequest] ([Id])
GO
ALTER TABLE [JOB].[EmailRequests] CHECK CONSTRAINT [FK_JobRequestID_EmailRequests] GO
ALTER TABLE [JOB].[ExecuteSqlRequests] WITH CHECK ADD CONSTRAINT [FK_JobRequestID_ExecuteSqlRequests] FOREIGN KEY([JobRequestId])
REFERENCES [JOB].[JobRequest] ([Id])
GO
ALTER TABLE [JOB].[ExecuteSqlRequests] CHECK CONSTRAINT [FK_JobRequestID_ExecuteSqlRequests]
GO
ALTER TABLE [JOB].[JobExecutionContextVariable] WITH CHECK ADD CONSTRAINT [FK_JobExecutionContextVariable_JobParamsLookup] FOREIGN KEY([JobParamId]) REFERENCES [JOB].[JobParamsLookup] ([JobParamId])
GO
ALTER TABLE [JOB].[JobExecutionContextVariable] CHECK CONSTRAINT [FK_JobExecutionContextVariable_JobParamsLookup]
GO
ALTER TABLE [JOB].[JobExecutionContextVariable] WITH CHECK ADD CONSTRAINT [FK_JobExecutionContextVariable_JobRequest] FOREIGN KEY([JobRequestId])
REFERENCES [JOB].[JobRequest] ([Id])
GO
ALTER TABLE [JOB].[JobExecutionContextVariable] CHECK CONSTRAINT [FK_JobExecutionContextVariable_JobRequest]
GO
ALTER TABLE [JOB].[JobRequest] WITH CHECK ADD CONSTRAINT [FK_JobRequest_Jobs] FOREIGN KEY([JobId])
REFERENCES [JOB].[Jobs] ([Id])
GO
ALTER TABLE [JOB].[JobRequest] CHECK CONSTRAINT [FK_JobRequest_Jobs]
GO
ALTER TABLE [JOB].[JobRequest] WITH CHECK ADD CONSTRAINT [FK_JobRequests_JobStatuses] FOREIGN KEY([JobStatusID])
REFERENCES [JOB].[JobStatuses] ([Id])
GO
ALTER TABLE [JOB].[JobRequest] CHECK CONSTRAINT [FK_JobRequests_JobStatuses]
GO
ALTER TABLE [JOB].[JobScheduleMap] WITH CHECK ADD CONSTRAINT [FK_JobScheduleMap_Jobs] FOREIGN KEY([JobId])
REFERENCES [JOB].[Jobs] ([Id])
GO
ALTER TABLE [JOB].[JobScheduleMap] CHECK CONSTRAINT [FK_JobScheduleMap_Jobs]
GO
ALTER TABLE [JOB].[JobScheduleMap] WITH CHECK ADD CONSTRAINT [FK_JobScheduleMap_Schedules] FOREIGN KEY([ScheduleId])

REFERENCES [JOB].[Schedules] ([ScheduleId])
GO
ALTER TABLE [JOB].[JobScheduleMap] CHECK CONSTRAINT [FK_JobScheduleMap_Schedules] GO
ALTER TABLE [JOB].[JobWorkflow]
FOREIGN KEY([JobId])
REFERENCES [JOB].[Jobs] ([Id])
GO
ALTER TABLE [JOB].[JobWorkflow]
GO
ALTER TABLE [JOB].[JobWorkflow]
FOREIGN KEY([TaskId])
REFERENCES [JOB].[Tasks] ([Id])
GO
ALTER TABLE [JOB].[JobWorkflow]
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] WITH CHECK ADD CONSTRAINT [FK_JobRequestID_JobWorkflowExecutionTracker] FOREIGN KEY([JobRequestId]) REFERENCES [JOB].[JobRequest] ([Id])
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] CHECK CONSTRAINT [FK_JobRequestID_JobWorkflowExecutionTracker]
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] WITH CHECK ADD CONSTRAINT [FK_JobWorkflowExecutionTracker_JobStatuses] FOREIGN KEY([ExecutionStatusId]) REFERENCES [JOB].[JobStatuses] ([Id])
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] CHECK CONSTRAINT [FK_JobWorkflowExecutionTracker_JobStatuses]
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] WITH CHECK ADD CONSTRAINT [FK_JobWorkflowExecutionTracker_Task] FOREIGN KEY([TaskId])
REFERENCES [JOB].[Tasks] ([Id])
GO
ALTER TABLE [JOB].[JobWorkflowExecutionTracker] CHECK CONSTRAINT [FK_JobWorkflowExecutionTracker_Task]
GO
WITH CHECK ADD CONSTRAINT [FK_JobWorkflow_Jobs]
CHECK CONSTRAINT [FK_JobWorkflow_Tasks]
CHECK CONSTRAINT [FK_JobWorkflow_Jobs]
WITH CHECK ADD CONSTRAINT [FK_JobWorkflow_Tasks]