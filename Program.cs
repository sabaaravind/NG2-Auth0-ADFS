using IFC.BEM.JobsFramework.TaskLib;
using IFC.BEM.Solutions.Common;
using IFC.BEM.Solutions.Common.Helpers;
using IFC.BEM.Solutions.Entities;
using IFC.BEM.Solutions.Mappings.NH;
using IFC.BEM.Solutions.Services.Contracts;
using log4net.Core;
using NHibernate;
using NHibernate.Linq;
using Quartz;
using Quartz.Impl;
using Quartz.Impl.Matchers;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;


namespace IFC.BEM.JobsFramework.Host
{
    class Program
    {
        #region Parameters
        public const int FAIL = 1;
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program));
        public const string ApplicationFriendlyName = "BEM Jobs";

        public static ISession StagingSession { get; set; }

        #endregion 

        #region Main Method

        public static int Main(string[] args)
        {
            try
            {
                //BEMLogger.LogMessage(string.Format("Started the BEMJobsFramework Session at {0} {1}", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), Level.Info, ApplicationFriendlyName, logger));
                Utils.EnvironmentDeciderForBatchApplications();
                DatabaseConfiguration.SetDatabaseEnvironment();
                BEMHelper.RegisterComponentsForBEMJobs();
            }

            catch (Exception ex)
            {
                BEMLogger.LogMessage("Error initializing Investran Staging connection.", ex, Level.Error, ApplicationFriendlyName, logger);
                return FAIL;
            }

            try
            {
                StagingSession = NHSessionManager.GetCurrentStagingSession(Utils.CurrentDBEnvironmentName);
                BEMLogger.LogMessage(string.Format("NH Session Created.."), Level.Info, ApplicationFriendlyName, logger);
            }
            catch (Exception ex)
            {
                BEMLogger.LogMessage(string.Format("Error while Creating NH Session .."), ex, Level.Error, ApplicationFriendlyName, logger);
                return FAIL;
            }
            try
            {

                int ReturnValue = 0;
                BEMLogger.LogMessage(string.Format("Started the BEMJobsFramework Session at {0} {1}", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), Level.Info, ApplicationFriendlyName, logger));
                BEMJobsHost.Start();
                return 0;
            }
            catch (Exception ex)
            {
                BEMLogger.LogMessage(string.Format("Error while Loading JobConfiguration"), ex, Level.Error, ApplicationFriendlyName, logger);
                return FAIL;
            }

            return 0;

        }
        #endregion
    }

    #region BEMJobsHost
    public static class BEMJobsHost
    {
        #region Parameters

        public const int FAIL = 1;
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program));
        public const string ApplicationFriendlyName = "BEM Jobs";

        #endregion 

        public static void Start()
        {
            try
            {
      
                IScheduler scheduler = StdSchedulerFactory.GetDefaultScheduler();

                BEMLogger.LogMessage(string.Format("Adding BEMJobsFramework Jobs"), Level.Info, ApplicationFriendlyName, logger);
                #region BEMJobsFramework Jobs

                BEMLogger.LogMessage(string.Format("Adding --> :BEM Job Scheduler"), Level.Info, ApplicationFriendlyName, logger);

                #region BEM Job Scheduler
                List<JobSchedule> jobSchedules = NHSessionManager.GetCurrentStagingSession(Utils.CurrentDBEnvironmentName).Query<JobSchedule>().ToList();

                IJobDetail bemJobScheduler = JobBuilder.Create<BEMJobScheduler>()
                                                        .WithIdentity("BEMJobScheduler", "Tasks")
                                                        .StoreDurably()
                                                        .Build();
                scheduler.AddJob(bemJobScheduler, true);

                foreach (JobSchedule jobSchedule in jobSchedules)
                {
                    Console.WriteLine(string.Format("Job={0}, Schedule={1}, Cron={2}", jobSchedule.JobName, jobSchedule.ScheduleName, jobSchedule.ScheduleExpression));

                    ITrigger jobTrigger = TriggerBuilder.Create()
                                                .WithCronSchedule(jobSchedule.ScheduleExpression)
                                                .WithIdentity(jobSchedule.ScheduleName, "Tasks")
                                                .UsingJobData("JobName", jobSchedule.JobName)
                                                .ForJob(bemJobScheduler)
                                                .Build();
                    scheduler.ScheduleJob(jobTrigger);

                }

                #endregion

                BEMLogger.LogMessage(string.Format("Adding --> :BEM Job Executor"), Level.Info, ApplicationFriendlyName, logger);
                #region BEM Job Executor

                IJobDetail bemJobExecutor = JobBuilder.Create<BEMJobExecutor>()
                                                                        .WithIdentity("BEMJobExecutor", "Tasks")
                                                                        .StoreDurably()
                                                                        .Build();


                ITrigger trgJobExecutor = TriggerBuilder.Create()
                                                    .WithDailyTimeIntervalSchedule
                                                    (
                                                        s => s.WithIntervalInSeconds(30)
                                                        .OnEveryDay()
                                                        .StartingDailyAt(TimeOfDay.HourAndMinuteOfDay(0, 0))
                                                    )
                                                    .WithIdentity("BEMJobExecutor", "Tasks")
                                                    .Build();

                scheduler.ScheduleJob(bemJobExecutor, trgJobExecutor);
                #endregion

                #endregion

                BEMLogger.LogMessage(string.Format("Adding Application Tasks"), Level.Info, ApplicationFriendlyName, logger);
                #region BEM Application Tasks

                BlankTask bt = new BlankTask();  // TEMP: Just to load the TaskLib Assembly into the domain.
                string appTaskLibAssemblyFullName = ConfigurationManager.AppSettings.Get("AppTaskLibAssemblyFullName");

                if (appTaskLibAssemblyFullName == null || appTaskLibAssemblyFullName == string.Empty)
                {
                    throw new Exception(string.Format("The App Settings Key {0} is missing", "AppTaskLibAssemblyFullName"));
                }

                var appTasksAssembly = (from asm in AppDomain.CurrentDomain.GetAssemblies()
                                    where asm.FullName == appTaskLibAssemblyFullName
                                    select asm).SingleOrDefault<Assembly>();

                if (appTasksAssembly == null)
                {
                    throw new Exception(string.Format("{0} assembly was not found in the current App Domain, please check the App Settings Key {1}", appTaskLibAssemblyFullName, "AppTaskLibAssemblyFullName"));
                }

                List<BEMTask> dbBEMTasks = NHSessionManager.GetCurrentStagingSession(Utils.CurrentDBEnvironmentName).Query<BEMTask>().ToList();
                List<Type> appDomainBEMTasks = appTasksAssembly.GetTypes().Where(t => t.GetInterfaces().Contains(typeof(IJob))).ToList();

                var appTasks = from appDomainBEMTask in appDomainBEMTasks
                               join dbBEMTask in dbBEMTasks 
                               on appDomainBEMTask.FullName equals dbBEMTask.ClassHandler
                               select new
                               {
                                   Task = appDomainBEMTask,
                                   Key = dbBEMTask.Name,
                                   Name = dbBEMTask.Name,
                                   ClassHandler = dbBEMTask.ClassHandler,
                                   DbProcHandler = dbBEMTask.DbProcHandler
                               };

                foreach (var appTask in appTasks)
                {
                    BEMLogger.LogMessage(string.Format("Adding --> : {0}", appTask.Name), Level.Info, ApplicationFriendlyName, logger);

                    JobKey _taskKey = new JobKey(appTask.Name, "Tasks");
                    IJobDetail _taskDetails = JobBuilder.Create(appTask.Task)
                                                      .WithIdentity(_taskKey)
                                                      .UsingJobData("DbProcHandler", appTask.DbProcHandler)
                                                      .UsingJobData("TaskName", appTask.Name)
                                                      .StoreDurably()
                                                      .Build();
                    scheduler.AddJob(_taskDetails, true);
                }

                #endregion

                #region Batch Job Listner
                var genericJobListner = new JobListner();
                genericJobListner.Name = "GenericJobListner";
                scheduler.ListenerManager.AddJobListener(genericJobListner, GroupMatcher<JobKey>.AnyGroup());
                #endregion

                BEMLogger.LogMessage(string.Format("Starting the scheduler."), Level.Info, ApplicationFriendlyName, logger);
                scheduler.Start();

                return;

            }
            catch (Exception exception)
            {
                BEMLogger.LogMessage(exception.Message, exception , Level.Fatal, ApplicationFriendlyName, logger);
            }

        }

    }
    #endregion

    #region BEMJobsFramework Jobs

    #region BEMJobScheduler
    public class BEMJobScheduler : IJob
    {
        #region Parameters
        public const int FAIL = 1;
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program));
        public const string ApplicationFriendlyName = "BEM Jobs";

        public static ISession StagingSession { get; set; }

        #endregion 
        public DateTime Schedule { get { return DateTime.Now; } private set { } }
        public string JobName { get; set; }


        public void Execute(IJobExecutionContext context)
        {
            //Console.WriteLine(string.Format("{0} : Scheduling the Job {1} at {2} ", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), JobName, Schedule));
            BEMLogger.LogMessage(string.Format("{0} : Scheduling the Job {1} at {2} ", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), JobName, Schedule), Level.Info, ApplicationFriendlyName, logger);

            try
            {
                SqlCommand _command = new SqlCommand(@"[JOB].[spInsertDIUJobRequest]");
                _command.Parameters.Add(new SqlParameter("@Jobname", JobName));
                _command.Parameters.Add(new SqlParameter("@ScheduleTime", DateTime.Now.ToString()));
                _command.CommandType = System.Data.CommandType.StoredProcedure;
                DatabaseCalls.ExecuteNonQuery(_command, "bemstg");
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }

        }
    }
    #endregion

    #region BEMJobExecutor
    public class BEMJobExecutor : IJob
    {
        #region Parameters
        public const int FAIL = 1;
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program));
        public const string ApplicationFriendlyName = "BEM Jobs";

        public static ISession StagingSession { get; set; }

        #endregion 
        public void Execute(IJobExecutionContext context)
        {
            DataTable _pendingJobRequests = null;
            int _jobRequestId = default(int);

            try
            {

                SqlCommand _command = new SqlCommand(@"SELECT * FROM [JOB].[vwJobRequests] WHERE JobStatus in ('New', 'Resume') AND ScheduleTime < GETDATE() order by JobRequestId");
                _command.CommandType = System.Data.CommandType.Text;
                _command.CommandTimeout = 1800;

                _pendingJobRequests = DatabaseCalls.GetDataTable(_command, "bemstg");


                if (_pendingJobRequests.Rows.Count > 0)
                    BEMLogger.LogMessage(string.Format("{0} : No. of Pending Jobs found : {1}", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), _pendingJobRequests.Rows.Count), Level.Info, ApplicationFriendlyName, logger);
                else
                    BEMLogger.LogMessage(string.Format("{0} : No. of Pending Jobs found : {1}", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), _pendingJobRequests.Rows.Count), Level.Debug, ApplicationFriendlyName, logger);

                foreach (DataRow _request in _pendingJobRequests.Rows)
                {
                    _jobRequestId = Int32.Parse(_request["JobRequestId"].ToString());
                    
                    BEMLogger.LogMessage(string.Format("Putting the JobRequest={0} in progress", _jobRequestId), Level.Info, ApplicationFriendlyName, logger);
                    DatabaseCalls.UpdateJobRequestStatus(_jobRequestId, (int)ExecutionStatus.Inprogress);

                    BEMLogger.LogMessage(string.Format("Initializing the work flow for Job Reqeuest {0} ", _jobRequestId), Level.Info, ApplicationFriendlyName, logger);
                    DatabaseCalls.InititializeJobWorkflow(_jobRequestId);

                    BEMLogger.LogMessage(string.Format("Getting the next task for Job Request {0} ", _jobRequestId), Level.Info, ApplicationFriendlyName, logger);
                    var _firstTaskDetails = DatabaseCalls.GetNextTaskInTheWorkflow(_jobRequestId, string.Empty);

                    // Trigger first task in the workflow.
                    if (_firstTaskDetails.Rows.Count == 1)
                    {
                        
                        var _firstTaskName = (_firstTaskDetails.Rows[0][1]).ToString();
                        BEMLogger.LogMessage(string.Format("The Next Task for Job Reqeuest ID {0} is {1}", _jobRequestId, _firstTaskName), Level.Info, ApplicationFriendlyName, logger);

                        var _firstTaskKey = (from _jobKey in context.Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("Tasks"))
                                     where _jobKey.Name == _firstTaskName
                                     select _jobKey).FirstOrDefault();

                        if (_firstTaskKey == null)
                        {
                            throw new JobExecutionException(string.Format("The Task was not found : {0}", _firstTaskName));
                        }

                        BEMLogger.LogMessage(string.Format("Adding the DataMap for JobRequestID = {0} and TaskName = {1}", _jobRequestId, _firstTaskName), Level.Info, ApplicationFriendlyName, logger);
                        JobDataMap _taskParams = new JobDataMap();
                        _taskParams.Add("JobRequestId", _jobRequestId);

                        #region Get Context Variables from DB, if any

                        var jobContextVariables = DatabaseCalls.GetJobExecutionContextVariables(_jobRequestId);

                        BEMLogger.LogMessage(string.Format("Number of Context Variables Found : {0}", jobContextVariables.Rows.Count), Level.Info, ApplicationFriendlyName, logger);
                        
                        string _mapKey = string.Empty;
                        string _mapValue = string.Empty;

                        foreach (DataRow _contextVariable in jobContextVariables.Rows)
                        {
                            _mapKey = _contextVariable[0].ToString();
                            _mapValue = _contextVariable[1].ToString();

                            
                            if (_taskParams.ContainsKey(_mapKey) == false)
                            {
                                BEMLogger.LogMessage(string.Format("Copying the key {0} with the value {1}", _mapKey, _mapValue), Level.Info, ApplicationFriendlyName, logger);
                                _taskParams.Add(_mapKey, _mapValue);
                            }
                        }

                        #endregion 

                        DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _firstTaskName, (int)ExecutionStatus.Inprogress);
                        DatabaseCalls.SetJobExecutionContextVariable(_jobRequestId, "JobRequestId", _jobRequestId.ToString());
                        context.Scheduler.TriggerJob(_firstTaskKey, _taskParams);

                    }
                    else
                    {
                        throw new Exception(string.Format("First Task not found for the JobRequestId : {0}", _jobRequestId));
                    }
                }
            }
            catch (Exception exception)
            {
                BEMLogger.LogMessage(string.Format("Error occured in the Task {0} : {1}", GetType().Name, exception.Message),  Level.Fatal, ApplicationFriendlyName, logger);
                throw new JobExecutionException(exception.Message);
            }
        }
    }
    #endregion

    #endregion

    #region BEMJobsFramework Listner
    public class JobListner : IJobListener
    {
        #region Parameters
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program));
        public const string ApplicationFriendlyName = "BEM Jobs";
        public string Name { get; set; }
        #endregion

        #region JobExecutionVetoed
        public void JobExecutionVetoed(IJobExecutionContext context)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region JobToBeExecuted
        public void JobToBeExecuted(IJobExecutionContext context)
        {

            try
            {
                string _taskName = context.JobDetail.Key.Name;
                
                if (_taskName == "BEMJobScheduler" || _taskName == "BEMJobExecutor")
                {
                    return;
                }
                int _jobRequestId = context.Trigger.JobDataMap.GetInt("JobRequestId");
                DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _taskName, (int)ExecutionStatus.Inprogress);
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message);
            }
        }
        #endregion

        #region JobWasExecuted

        public void JobWasExecuted(IJobExecutionContext context, JobExecutionException origJobException)
        {
            #region Variables
            string _jobName = string.Empty;
            string _currentTaskName = string.Empty;
            string _nextTaskName = string.Empty;
            DataTable _nextTaskDetails = null;
            ExecutionStatus _currentTaskStatus = ExecutionStatus.Failed;
            string _mapKey = string.Empty;
            string _mapValue = string.Empty;
            JobDataMap nextTaskMap = null;
            string _exceptionStackTrace = string.Empty;
            int _jobRequestId = 0;

            #endregion
            try
            {
                _currentTaskName = context.JobDetail.Key.Name;

                #region BEMJobsFramework Jobs
                if (_currentTaskName == "BEMJobScheduler" || _currentTaskName == "BEMJobExecutor")
                {
                    if (origJobException != null)
                    {
                        BEMLogger.LogMessage(string.Format("Either BEMJobScheduler or BEMJobExecutor encountered a fatal error {0}", origJobException.Message), Level.Fatal, ApplicationFriendlyName, logger);
                        _exceptionStackTrace = origJobException.Message + " : " + origJobException.StackTrace;
                        _currentTaskStatus = ExecutionStatus.Failed;
                    }
                    else
                    {
                        _currentTaskStatus = ExecutionStatus.Complete;
                    }
                    return;
                }
                #endregion

                _jobRequestId = context.Trigger.JobDataMap.GetInt("JobRequestId");
                BEMLogger.LogMessage(string.Format("Job Executed -> JobRequestID : {0}, Task Name {1}", _jobRequestId, _currentTaskName), Level.Info, ApplicationFriendlyName, logger);

                if (origJobException != null)
                    BEMLogger.LogMessage(string.Format("Exception Message {0}, Exception Stack Trace {1}", origJobException.Message, origJobException.StackTrace), Level.Info, ApplicationFriendlyName, logger);


                #region Deduce the current  task's status.
                if (origJobException == null)
                {
                    // No Exception
                    BEMLogger.LogMessage(string.Format("No Exception"), Level.Debug, ApplicationFriendlyName, logger);
                    _currentTaskStatus = ExecutionStatus.Complete;
                }else  if (origJobException.InnerException == null)
                {
                    // No Inner Exeptoin, the task failed
                    BEMLogger.LogMessage(string.Format("No Inner Exception, the task failed"), Level.Info, ApplicationFriendlyName, logger);
                    _currentTaskStatus = ExecutionStatus.Failed;
                }
                else if (origJobException.InnerException.GetType().IsSubclassOf(typeof(JobExecutionException)) == true)
                {
                    BEMLogger.LogMessage(string.Format("Known Exception"), Level.Info, ApplicationFriendlyName, logger);
                    BEMLogger.LogMessage(origJobException.InnerException.GetType().Name, Level.Info, ApplicationFriendlyName, logger);

                    switch (origJobException.InnerException.GetType().Name)
                    {
                        case "JobPauseException":
                            _currentTaskStatus = ExecutionStatus.InWaiting;
                            break;
                        case "JobAbortException":
                            _currentTaskStatus = ExecutionStatus.Abort;
                            break;
                        default:
                            _currentTaskStatus = ExecutionStatus.Failed;
                            break;
                    }
                }
                else 
                {
                    // Unknown Exception
                    BEMLogger.LogMessage(string.Format("Unknown Exception"), Level.Info, ApplicationFriendlyName, logger);
                    _currentTaskStatus = ExecutionStatus.Failed;
                }

                #endregion

                BEMLogger.LogMessage(string.Format("The Task Status is {0}", _currentTaskStatus.ToString()), Level.Info, ApplicationFriendlyName, logger);

                #region If BEMTask "Failed"
                if (_currentTaskStatus == ExecutionStatus.Failed)
                {
                    _exceptionStackTrace = origJobException.Message + " : " + origJobException.StackTrace;
                    BEMLogger.LogMessage(string.Format("The Task {0} Execution Errorred out with the Exception {1}. Exeption Type {2}", _currentTaskName, origJobException.Message, origJobException.GetType().Name), origJobException, Level.Error, ApplicationFriendlyName, logger);
                    BEMLogger.LogMessage(string.Format("{0}-{1}-{2}", _jobRequestId, _currentTaskName, ExecutionStatus.Failed), origJobException, Level.Error, ApplicationFriendlyName, logger);

                    DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _currentTaskName, (int)ExecutionStatus.Failed);
                    DatabaseCalls.UpdateJobRequestStatus(_jobRequestId, (int)ExecutionStatus.Failed);
                    // Do not progress the work flow and exit.
                    return;
                }
                #endregion

                #region If BEMTask "Success"
                if (_currentTaskStatus == ExecutionStatus.Complete)
                {
                    BEMLogger.LogMessage(string.Format("The Job {0} Execution completed successfully.Trigger MapCount is {1}, Job MapCount {2}", _currentTaskName, context.Trigger.JobDataMap.Count, context.JobDetail.JobDataMap.Count), Level.Info, ApplicationFriendlyName, logger);
                    DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _currentTaskName, (int)ExecutionStatus.Complete);
                    _nextTaskDetails = DatabaseCalls.GetNextTaskInTheWorkflow(_jobRequestId, _currentTaskName);

                    if (_nextTaskDetails.Rows.Count == 1)
                    {

                        _nextTaskName = (_nextTaskDetails.Rows[0][1]).ToString();

                        BEMLogger.LogMessage(string.Format("Next Task Name is {0}", _nextTaskName), Level.Info, ApplicationFriendlyName, logger);

                        var _nextTask = (from _jobKey in context.Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("Tasks"))
                                         where _jobKey.Name == _nextTaskName
                                         select _jobKey).FirstOrDefault();

                        if (_nextTask == null)
                        {
                            throw new Exception(_nextTaskName + " is missing from Tasks List ,please check and correct the task name.");
                        }

                        var jobContextVariables = DatabaseCalls.GetJobExecutionContextVariables(_jobRequestId);
                        nextTaskMap = new JobDataMap();

                        BEMLogger.LogMessage(string.Format("Number of Context Variables Found : {0}", jobContextVariables.Rows.Count), Level.Info, ApplicationFriendlyName, logger);
                        
                        foreach(DataRow _contextVariable in jobContextVariables.Rows)
                        {
                            _mapKey = _contextVariable[0].ToString();
                            _mapValue = _contextVariable[1].ToString();

                            BEMLogger.LogMessage(string.Format("Copying the key {0} with the value {1}", _mapKey, _mapValue), Level.Info, ApplicationFriendlyName, logger);
                            nextTaskMap.Add(_mapKey, _mapValue);
                        }


                        DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _nextTaskName, (int)ExecutionStatus.Inprogress);
                        context.Scheduler.TriggerJob(_nextTask, nextTaskMap);
                    }
                    else
                    {
                        // If there is no next task & there is no exception, mark the last task as well as the whole Job as complete.
                        DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _currentTaskName, (int)ExecutionStatus.Complete);
                        DatabaseCalls.UpdateJobRequestStatus(_jobRequestId, (int)ExecutionStatus.Complete);
                    }
                }
                #endregion

                #region If task is in "In-Waiting"
                if (_currentTaskStatus == ExecutionStatus.InWaiting)
                {
                    DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _currentTaskName, (int)ExecutionStatus.Inprogress);
                    DatabaseCalls.UpdateJobRequestStatus(_jobRequestId, (int)ExecutionStatus.Paused);
                }
                #endregion

                #region If task Status is "Abort"
                if (_currentTaskStatus == ExecutionStatus.Abort)
                {
                    _exceptionStackTrace = origJobException.Message + " : " + origJobException.StackTrace;
                    // Mark the current BEMTask as Complete.
                    DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _currentTaskName, (int)ExecutionStatus.Complete);

                    // Mark all next tasks as Cancelled.
                    do
                    {
                        _nextTaskDetails = DatabaseCalls.GetNextTaskInTheWorkflow(_jobRequestId, _currentTaskName);

                        if (_nextTaskDetails.Rows.Count == 0)
                            break;

                        _nextTaskName = (_nextTaskDetails.Rows[0][1]).ToString();
                        DatabaseCalls.UpdateJobWorkflowExecutionTracker(_jobRequestId, _nextTaskName, (int) ExecutionStatus.Abort);

                        _currentTaskName = _nextTaskName;

                    } while (1 == 1);


                    DatabaseCalls.UpdateJobRequestStatus(_jobRequestId, (int)ExecutionStatus.Complete);
                }
                #endregion

            }
            catch (Exception exception)
            {
                _currentTaskStatus = ExecutionStatus.Failed;
                BEMLogger.LogMessage(string.Format("Task Listner Failed : The Task {0} Execution Errorred out with the Exception {1}. Exeption Type {2}", _currentTaskName, exception.Message, exception.GetType().Name), Level.Fatal, ApplicationFriendlyName, logger);
                throw;
            }
            finally
            {
                try
                {
                    #region Send Job Failed Notification
                    if (_currentTaskStatus == ExecutionStatus.Failed || _currentTaskStatus == ExecutionStatus.Abort)
                    {

                        string _messageBody = string.Empty;
                        string _jobTasks = string.Empty;
                        string _jobExeuctionParams = string.Empty;
                        string _jobExecutionLog = string.Empty;

                        _messageBody = string.Format("{0}    {1}", BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("HTMLTableCSSForEmail").Value, BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("BEMJobsEmailAlertToSupportTeam").Value); 

                        _jobExeuctionParams = DatabaseCalls.GetJobExecutionContextVariables(_jobRequestId).ConvertDataTableToHTML();
                        _jobTasks = DatabaseCalls.GetTasksByJobRequestId(_jobRequestId).ConvertDataTableToHTML();

                        _messageBody = _messageBody.Replace("@TaskStatus", _currentTaskStatus.ToString())
                                                    .Replace("@ExceptionStackTrace", _exceptionStackTrace)
                                                    .Replace("@JobTasks", _jobTasks)
                                                    .Replace("@JobExecutionParameters", _jobExeuctionParams)
                                                    .Replace("@JobExecutionLog", _jobExecutionLog)
                                                    .Replace("@JobRequestId", _jobRequestId.ToString());

                        BEMLogger.LogMessage(string.Format("  Current Task Status - {0}", _currentTaskStatus), Level.Debug, ApplicationFriendlyName, logger);
                        int emailJobRequestId = DatabaseCalls.CreateEmailJobRequest(
                             BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("BEMJobsFrameworkFailureEmailAlertDL").Value,
                             "High",
                             string.Format("BEM Jobs Error : The Job Request ID {0} {1}!", _jobRequestId, _currentTaskStatus.ToString()),
                             _messageBody,
                             "Html"
                         );
                        BEMLogger.LogMessage(string.Format("Creating Email Job {0}",emailJobRequestId), Level.Debug, ApplicationFriendlyName, logger);
                    }
                    #endregion
                }
                catch (Exception exception)
                {
                    BEMLogger.LogMessage(exception.Message, exception, Level.Warn, ApplicationFriendlyName, logger);
                }
            }

            return;
        }

        #endregion
    }
    #endregion
    
}
