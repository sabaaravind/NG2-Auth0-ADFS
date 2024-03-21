using System;
using System.Collections.Generic;
using System.Text;
using Quartz;
using log4net.Core;
using System.Configuration;
using IFC.BEM.Solutions.Common;
using System.Reflection;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using IFC.BEM.Solutions.Common.Helpers;
using Sungard.Investran.Suite.DataImport.WebServices.Contracts;
using IFC.Investran7x;
using Sungard.Investran.Suite.WebServices.Contracts;
using Sungard.Investran.Suite.WebServices.Contracts.Common.Lookups;
using Sungard.Investran.Suite.WebServices.Contracts.Scheduler;
using System.Threading;
using IFC.BEM.JobsFramework.BEMJobsKnownExceptions;
using IFC.BEM.Solutions.Entities;
using IFC.BEM.Solutions.Services.Contracts;
using IFC.BEM.Solutions.Mappings.NH;
using NHibernate.Linq;
using System.IO;
using WinSCP;
using System.Linq;

namespace IFC.BEM.JobsFramework.TaskLib
{

    #region BEM Jobs Base Class
    public class BEMBaseTask
    {
        public static readonly log4net.ILog logger = log4net.LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        public const string ApplicationFriendlyName = "Bem Jobs";

        public int JobRequestId { get; set; }

        #region DIUTemplateName
        private string _propDIUTemplateName = string.Empty;
        public string DIUTemplateName
        {
            get
            {
                return _propDIUTemplateName;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "DIUTemplateName", value);
                _propDIUTemplateName = value;
            }
        }
        #endregion

        #region SSRSReportName
        private string _propSSRSReportName = string.Empty;
        public string SSRSReportName
        {
            get
            {
                return _propSSRSReportName;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "SSRSReportName", value);
                _propSSRSReportName = value;
            }
        }
        #endregion

        #region DIUProcessID
        string _propDIUProcessID = string.Empty;
        public string DIUProcessID
        {
            get
            {
                return _propDIUProcessID;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "DIUProcessID", value);
                _propDIUProcessID = value;
            }

        }
        #endregion

        #region ObjectIDs
        private string _propObjectIDs = string.Empty;
        public string ObjectIDs
        {
            get
            {
                return _propObjectIDs;
            }
            set
            {
                _propObjectIDs = value;
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "ObjectIDs", value);
            }
        }
        #endregion

        #region DIUExcelFilePath
        string _propDIUExcelFilePath = string.Empty;
        public string DIUExcelFilePath
        {
            get
            {
                return _propDIUExcelFilePath;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "DIUExcelFilePath", value);
                _propDIUExcelFilePath = value;
            }
        }
        #endregion

        #region DataTable
        string _dataTable = string.Empty;
        public string DataTable
        {
            get
            {
                return _dataTable;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "DataTable", value);
                _dataTable = value;
            }
        }
        #endregion

        #region FilePath
        string _filePath = string.Empty;
        public string FilePath
        {
            get
            {
                return _filePath;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "FilePath", value);
                _filePath = value;
            }
        }
        #endregion

        #region SAPFileName
        string _sapFileName = string.Empty;
        public string SAPFileName
        {
            get
            {
                return _sapFileName;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "SAPFileName", value);
                _sapFileName = value;
            }
        }
        #endregion

        #region FTPProfileName
        string _ftpProfileName = string.Empty;
        public string FTPProfileName
        {
            get
            {
                return _ftpProfileName;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "FTPProfileName", value);
                _ftpProfileName = value;
            }
        }
        #endregion

        #region FTPFilePath
        string _ftpFilePath = string.Empty;
        public string FTPFilePath
        {
            get
            {
                return _ftpFilePath;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "FTPFilePath", value);
                _ftpFilePath = value;
            }
        }
        #endregion

        #region EmailAttachmentFilePath
        string _emailAttachmentFilePath = string.Empty;
        public string EmailAttachmentFilePath
        {
            get
            {
                return _emailAttachmentFilePath;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "EmailAttachmentFilePath", value);
                _emailAttachmentFilePath = value;
            }
        }
        #endregion

        #region HtmlTable
        string _htmlTable = string.Empty;
        public string HtmlTable
        {
            get
            {
                return _htmlTable;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "HtmlTable", value);
                _htmlTable = value;
            }
        }
        #endregion

        #region RecordCount
        string _recordCount = string.Empty;
        public string RecordCount
        {
            get
            {
                return _recordCount;
            }
            set
            {
                DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "RecordCount", value);
                _recordCount = value;
            }

        }
        #endregion

    }
    #endregion

    #region BEM Application Jobs

    #region DCRTasks
    [DisallowConcurrentExecution]
    public class StartDCRTask : BEMBaseTask, IJob
    {

        #region Methods

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                DIUTemplateName = ConfigurationManager.AppSettings.Get("DIUDCRTemplate");
                SSRSReportName = ConfigurationManager.AppSettings.Get("SSRSDCRReport");
            }
            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }
        }
        #endregion

    }

    [DisallowConcurrentExecution]
    public class CompleteDCRTask : BEMBaseTask, IJob
    {
        #region Methods

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                if (DIUProcessID == string.Empty)
                {
                    BEMLogger.LogMessage("Process ID Not found.", null, Level.Error, ApplicationFriendlyName, logger);
                    throw new Exception("Process ID Not found.");
                }

                SqlCommand _command = new SqlCommand();
                _command.CommandText = "[BEM].[DIU_CheckDCRDataLoadJobStatus]";
                _command.CommandType = CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;
                _command.Parameters.Add(new SqlParameter("@JobrequestID", JobRequestId));
                _command.Parameters.Add(new SqlParameter("@ProcessID", Int32.Parse(DIUProcessID)));

                using (SqlConnection _connection = new SqlConnection(ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                {
                    _connection.Open();
                    _command.Connection = _connection;
                    _command.ExecuteNonQuery();
                }
            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }
        }

        #endregion
    }

    #endregion

    #region UserTask
    [DisallowConcurrentExecution]
    public class UserTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {

            try
            {
                throw new TaskInWaitingException("Waiting for User Action");
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }

            // This task has to throw TaskInWaitingException so that Workflow can wait for User Action.
            // It does below things.
            // #1. BEMTask goes to "In-Waiting" status
            // #2. Job goes to "Pause" status
            // Once User Takes action, it(the action) needs to put this BEMTask back to "Completed" status and Job Status to "Resume", 
            // so that workflow can continue.

        }
    }


    #endregion

    #region CompleteASLValuationTask
    [DisallowConcurrentExecution]
    public class CompleteASLValuationTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                // Check the User Action on the ASL-Valuation Event.

                // If Action == Delete EXIT
                // If Action == Complete Schedule Valuation Method UDF Upload

                string userAction = DatabaseCalls.GetJobExecutionContextVariable(JobRequestId, "UserActionOnASLValuation");

                if (userAction.ToUpper() == "DELETE")
                {
                    throw new JobAbortException("ASL User Action : DELETE");
                }

                if (userAction.ToUpper() == "SUBMIT")
                {
                    return;
                }

                if (userAction.ToUpper() == "COMPLETE")
                {
                    // Get UDF SSRS Report Name
                    // Compose the SSRS Object
                    SSRSReport _ssrsReport = new SSRSReport();

                    _ssrsReport.ServerUrl = BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("ReportServerURL").Value;
                    _ssrsReport.Path = BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("BEMJobsSSRSReportFolder").Value;

                    string ssrsReportRequestXml = string.Empty;
                    ssrsReportRequestXml = Helper.GetXml(_ssrsReport);
                    DatabaseCalls.SetJobExecutionContextVariable(this.JobRequestId, "SSRSReportName", ConfigurationManager.AppSettings.Get("SpecPositionUDFReport"));


                    return;
                }
                else
                {
                    throw new Exception("Unknown User Action on the ASL-Valuation Event");
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }


    #endregion

    #region ASLValuationLogExclusions
    [DisallowConcurrentExecution]
    public class ASLValuationLogExclusions : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                foreach (string _objectId in ObjectIDs.Split(','))
                {
                    SqlCommand _command = new SqlCommand("[dbo].[USP_GET_ASL_VALUATION_EXCLUSIONS]");
                    _command.Parameters.Add(new SqlParameter("@Object_ID", Int32.Parse(_objectId)));
                    _command.CommandType = CommandType.StoredProcedure;
                    _command.CommandType = System.Data.CommandType.StoredProcedure;
                    _command.CommandTimeout = 1800;
                    var result = DatabaseCalls.ExecuteNonQuery(_command, "investran");
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region ASLValuationUpdateValuationUDFs
    [DisallowConcurrentExecution]
    public class ASLValuationUpdateValuationUDFs : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                SqlCommand _command = new SqlCommand("[dbo].[BEM_spValuationUDFUpdate]");
                _command.Parameters.Add(new SqlParameter("@JobRequestId", JobRequestId));
                _command.CommandType = CommandType.StoredProcedure;
                _command.CommandType = System.Data.CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;
                var result = DatabaseCalls.ExecuteNonQuery(_command, "bemstg");
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region ASLValuationEmailStatistics
    [DisallowConcurrentExecution]
    public class ASLValuationEmailStatistics : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                foreach (string _objectId in ObjectIDs.Split(','))
                {
                    SqlCommand _command = new SqlCommand("[dbo].[USP_getASLValutionEventStatistics]");
                    _command.Parameters.Add(new SqlParameter("@Object_ID", Int32.Parse(_objectId)));
                    _command.CommandType = CommandType.StoredProcedure;
                    _command.CommandTimeout = 1800;
                    _command.CommandType = System.Data.CommandType.StoredProcedure;
                    var result = DatabaseCalls.ExecuteNonQuery(_command, "bemstg");
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region BlankTask

    [DisallowConcurrentExecution]
    public class BlankTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                //throw new Exception("Dummy Exception");
                //throw new JobPauseException("Waiting for user Input");
                throw new JobAbortException("There is no data to process");
            }

            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }

        }
    }


    #endregion

    #region CompleteBatchEventTask
    [DisallowConcurrentExecution]
    public class CompleteBatchEventTask : BEMBaseTask, IJob
    {
        #region Execute

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                #region Execute

                int _eventCounter = 0;


                foreach (string _objectId in ObjectIDs.Split(','))
                {
                    _eventCounter++;

                    SqlCommand _command = new SqlCommand(@"[JOB].[spCompleteBatchEvent]");
                    _command.Parameters.Add(new SqlParameter("@ObjectID", Int32.Parse(_objectId)));
                    _command.Parameters.Add(new SqlParameter("@diuProcessId", DIUProcessID));
                    _command.Parameters.Add(new SqlParameter("@IsFinalEvent", (_eventCounter == ObjectIDs.Split(',').Length ? 1 : 0)));
                    _command.CommandType = System.Data.CommandType.StoredProcedure;
                    _command.CommandTimeout = 1800;
                    var result = DatabaseCalls.ExecuteNonQuery(_command, "bemstg");

                }
                #endregion
            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }

        }

        #endregion
    }

    #endregion

    #region BatchProcessTask

    [DisallowConcurrentExecution]
    public class BatchProcessTask : BEMBaseTask, IJob
    {

        #region Execute
        public void Execute(IJobExecutionContext context)
        {
            BEMLogger.LogMessage(string.Format("{0} : Executing the Job Request: {1} with ID {2}", DateTime.Now.ToString("hh:mm:ss:ms", CultureInfo.InvariantCulture), GetType().Name, JobRequestId), Level.Info, ApplicationFriendlyName, logger);

            DataTable objectIDs = null;
            StringBuilder _objectIdsCsv = new StringBuilder();

            try
            {
                #region Execute

                objectIDs = DatabaseCalls.FetchBatchData(JobRequestId, DateTime.Now);

                if (objectIDs == null || objectIDs.Rows.Count == 0)
                {
                    throw new JobExecutionException(string.Format("The Batch Process hasn't generated any trasactions"));
                }

                BEMLogger.LogMessage(string.Format("The Batch process had generated {0} number of ObjectIDs.", objectIDs.Rows.Count), null, Level.Info, ApplicationFriendlyName, logger, this.JobRequestId, GetType().Name);

                foreach (DataRow _row in objectIDs.Rows)
                {
                    _objectIdsCsv.Append(_row[0].ToString() + ",");
                }
                string objectIdsCsv = _objectIdsCsv.ToString();

                objectIdsCsv = _objectIdsCsv.ToString().Substring(0, _objectIdsCsv.ToString().Length - 1);
                #endregion

                // OUTPUT Variables
                DIUTemplateName = ConfigurationManager.AppSettings.Get("DIUBatchProcessTemplate"); ;
                SSRSReportName = ConfigurationManager.AppSettings.Get("SSRSBatchProcessReport");
                ObjectIDs = objectIdsCsv;

            }
            catch (JobExecutionException jeException)
            {
                throw jeException;
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }

        }
        #endregion
    }

    #endregion

    #region ETFTasks
    public class StartETFTask : BEMBaseTask, IJob
    {
        #region Methods

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                DIUTemplateName = ConfigurationManager.AppSettings.Get("DIUETFTemplate"); ;
                SSRSReportName = ConfigurationManager.AppSettings.Get("SSRSETFReport");
            }
            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }
        }
        #endregion

    }

    public class CompleteETFTask : BEMBaseTask, IJob
    {

        #region Methods

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                int diuProcessID = default(int);
                diuProcessID = Int32.Parse(DIUProcessID);
                if (diuProcessID == 0)
                {
                    BEMLogger.LogMessage("Process ID Not found.", null, Level.Error, ApplicationFriendlyName, logger);
                    throw new Exception("Process ID Not found.");
                }

                SqlCommand _command = new SqlCommand();
                _command.CommandType = CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;

                // INVOKE SP to update status
                _command.CommandText = "[JOB].[USP_Load_ETF_Data_Email_Status]";
                _command.Parameters.Add(new SqlParameter("@ExecStatus", 'C'));
                _command.Parameters.Add(new SqlParameter("@Message", ""));

                using (SqlConnection _connection = new SqlConnection(ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                {
                    _connection.Open();
                    _command.Connection = _connection;
                    _command.ExecuteNonQuery();
                }

            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }

        }

        #endregion

    }

    #endregion

    #region CreateExcelFromSSRSJob

    [DisallowConcurrentExecution]
    public class CreateExcelFromSSRSJob : BEMBaseTask, IJob
    {
        #region Execute
        public void Execute(IJobExecutionContext context)
        {

            string filePath = string.Empty;
            FileStream oFileStream = null;
            string reportServerURL = string.Empty;
            string reportPath = string.Empty;
            string excelFileName = string.Empty;
            Byte[] bytes = default(Byte[]);
            Microsoft.Reporting.WinForms.ReportViewer reportViewer = null;

            try
            {

                BEMLogger.LogMessage("SSRS Report Name : " + SSRSReportName, null, Level.Info, ApplicationFriendlyName, logger, this.JobRequestId, GetType().Name);
                reportViewer = new Microsoft.Reporting.WinForms.ReportViewer();

                reportServerURL = BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("ReportServerURL").Value;
                reportPath = string.Format("{0}{1}", BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("BEMJobsSSRSReportFolder").Value, SSRSReportName);

                reportViewer.ProcessingMode = Microsoft.Reporting.WinForms.ProcessingMode.Remote;
                reportViewer.ServerReport.ReportServerUrl = new Uri(reportServerURL);
                reportViewer.ServerReport.ReportPath = reportPath;
                reportViewer.ServerReport.SetParameters(new Microsoft.Reporting.WinForms.ReportParameter("JobRequestId", JobRequestId.ToString()));

                BEMLogger.LogMessage(string.Format("SSRS Settings:: ServerURL={0}, ReportPath={1}", reportServerURL, reportPath), Level.Info, ApplicationFriendlyName, logger);

                excelFileName = string.Format("JobRequestID_{0}_{1}.xlsx", JobRequestId, DateTime.Now.ToString("MM-dd-yyyy_hh-mm-ss-ms"));
                filePath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location) + "\\SSRSExcelReports\\" + excelFileName;
                BEMLogger.LogMessage(string.Format("File Path ={0}", filePath), Level.Info, ApplicationFriendlyName, logger);


                bytes = reportViewer.ServerReport.Render("EXCELOPENXML");
                BEMLogger.LogMessage(string.Format("SSRS Report Render Completed."), Level.Info, ApplicationFriendlyName, logger);

                if (bytes == null)
                    BEMLogger.LogMessage(string.Format("No Bytes received"), Level.Warn, ApplicationFriendlyName, logger);
                else
                    BEMLogger.LogMessage(string.Format("SSRS Report Render Completed. Bytes length {0}", bytes.LongLength), Level.Info, ApplicationFriendlyName, logger);

                oFileStream = new System.IO.FileStream(filePath, System.IO.FileMode.Create);
                oFileStream.Write(bytes, 0, bytes.Length);
                oFileStream.Close();

                // OUTPUT Variables
                DIUExcelFilePath = filePath;

            }
            catch (JobExecutionException jeException)
            {
                throw jeException;
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
            finally
            {
                reportViewer.Dispose();
            }

        }
        #endregion
    }

    #endregion

    #region InvokeDIUTask

    [DisallowConcurrentExecution]
    public class InvokeDIUTask : BEMBaseTask, IJob
    {
        #region Execute

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                int diuProcessId = default(int);

                #region Execute

                try
                {
                    diuProcessId = DIUTask(DIUExcelFilePath, DIUTemplateName);

                    if (diuProcessId == 0)
                    {
                        BEMLogger.LogMessage("DIU Process ID =" + diuProcessId, Level.Error, this, logger);
                        throw new Exception("DIU Process ID can not be zero");
                    }
                }
                catch (JobExecutionException jeException)
                {
                    throw jeException;
                }
                catch (Exception exception)
                {
                    throw new JobExecutionException(exception.Message, exception);
                }
                #endregion
            }
            catch (Exception ex)
            {
                BEMLogger.LogMessage("Error while executing Execute methdod in InvokeDIUTask.", ex, Level.Error, ApplicationFriendlyName, logger);
                throw ex;
            }

        }

        #endregion

        #region Private Method

        private int DIUTask(string filePath, string templateName)
        {
            int processId = 0;
            try
            {
                BEMHelper.RegisterComponentsForBEMJobs();
                DataImportJobDto TempID = InvestranDataImportJobServices<DataImportJobDto>.GetByName(templateName);

                if (TempID == null)
                {
                    throw new Exception(string.Format("DIU Template Not found : {0}", templateName));
                }

                int TemplateId = TempID.Id;

                string options = FetchOptions.AsString(FetchOptions.AsBigInteger(DataImportFetchOptions.DataImportFileSheetMappings)
                                                    | FetchOptions.AsBigInteger(DataImportFetchOptions.DataImportJobFileSheets));

                ExcelDataImportJobDto template = InvestranDataImportJobServices<ExcelDataImportJobDto>.GetById(TemplateId, options);

                BEMLogger.LogMessage(string.Format("template ID {0}, template Name {1} - {2}", template.Id, template.Name, template.GetType().Name), null, Level.Info, ApplicationFriendlyName, logger);

                var investranDomain = template.Domain;

                int dataImportEntityTypeId = 100;

                EntityTypeDto entityType = InvestranLookupServices<EntityTypeDto>.GetById(dataImportEntityTypeId);

                // Create an Excel import job.
                ExcelDataImportJobDto importJob = new ExcelDataImportJobDto
                {
                    Domain = investranDomain,
                    EntityType = entityType,
                    File = InvestranHelper.CreateUploadedFile(filePath),
                    Template = false,
                    State = DataImportJobStateDto.InProcess,
                    PublicTemplate = false,
                    Loaded = true
                };


                importJob.FileSheets = ApplyTemplate(template.FileSheets);

                importJob.FileOptions = new ExcelDataImportFileOptionsDto { SkippedRows = 0, Entity = importJob };

                importJob.ImportJobOptions = new DataImportJobOptionsDto
                {
                    AddNewLookupValues = true,
                    SkipOnError = false,
                    AllowUpdates = true,
                    IgnoreCriticalWarnings = false,
                    IgnoreEmptyFields = true
                };


                importJob.Name = template.Name + " - " + DateTime.Now;

                int jobId = 0;

                ProcessInformationDto savedJob = InvestranDataImportJobServices<DataImportJobDto>.PublishAndGetProcess(importJob);

                processId = savedJob.Id;

                DIUProcessID = processId.ToString();

                BEMLogger.LogMessage(string.Format("processId: {0}, jobId: {1}", processId, jobId), null, Level.Info, ApplicationFriendlyName, logger);

                ProcessInformationDto processInformation = InvestranHelper.GetProcessInformation(processId);

                DateTime start = DateTime.Now;
                DateTime current = DateTime.Now;
                double maxWaitInMinutes = 240d;
                int diuPollingInterval = 30000;

                Double.TryParse(BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("DIUMaxWaitInMinutes").Value, out maxWaitInMinutes);
                Int32.TryParse(BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("DIUJobStatusPollingIntervalInMilliSeconds").Value, out diuPollingInterval);

                bool stop = false;
                var executing = processInformation == null ||
                                (processInformation.ProcessStatus != ProcessStatusDto.Canceled
                                && processInformation.ProcessStatus != ProcessStatusDto.ExecutedWarnings &&
                                processInformation.ProcessStatus != ProcessStatusDto.Failed
                                && processInformation.ProcessStatus != ProcessStatusDto.Succeeded);


                var span = current - start;

                while (executing)
                {
                    span = current - start;
                    stop = span.TotalMinutes > maxWaitInMinutes;
                    if (stop)
                    {

                        BEMLogger.LogMessage(string.Format("processId: {0}, jobId: {1}", processId, jobId), null, Level.Info, ApplicationFriendlyName, logger);
                        BEMLogger.LogMessage(string.Format("DIU process has not completed after at least [{0}] minutes. will stop monitoring. Check results of process in Investran.", maxWaitInMinutes), null, Level.Info, ApplicationFriendlyName, logger);
                        throw new Exception(string.Format("DIU process has not completed after at least [{0}] minutes. Will stop monitoring. Failing the both the Job and Task. Please check results of process in Investran CRM and resume the job if everything looks good. Import Job Name = {1}, ProcessID= {2}", maxWaitInMinutes, importJob.Name, processId));
                    }
                    else
                    {
                        BEMLogger.LogMessage(string.Format("DIU Process [{0}] Executing... elapsed time [{1}] seconds...", processId, span.TotalSeconds), null, Level.Info, ApplicationFriendlyName, logger);
                    }
                    Thread.Sleep(diuPollingInterval);

                    processInformation = InvestranHelper.GetProcessInformation(processId);

                    executing = processInformation == null ||
                                (processInformation.ProcessStatus != ProcessStatusDto.Canceled
                                && processInformation.ProcessStatus != ProcessStatusDto.ExecutedWarnings &&
                                processInformation.ProcessStatus != ProcessStatusDto.Failed
                                && processInformation.ProcessStatus != ProcessStatusDto.Succeeded);

                    current = DateTime.Now;
                }

                if (!executing)
                {
                    span = current - start;

                    BEMLogger.LogMessage(string.Format("processId: {0}, jobId: {1}", processId, jobId), Level.Info, ApplicationFriendlyName, logger);
                    BEMLogger.LogMessage(string.Format("Final Status: [{0}].", processInformation.ProcessStatus), Level.Info, ApplicationFriendlyName, logger);
                    BEMLogger.LogMessage(string.Format("Total Execution Time [{0}] seconds...", span.TotalSeconds), Level.Info, ApplicationFriendlyName, logger);
                }

                if (processInformation.ProcessStatus != ProcessStatusDto.Succeeded)
                {
                    //BEMLogger.LogMessage("Error in Invoke DIU BEMTask. ", null, Level.Error, ApplicationFriendlyName, logger);
                    //throw new Exception(string.Format("The DIU Job {0} is still in {1} state after {2} minutes", templateName, processInformation.ProcessStatus.ToString(), maxWaitInMinutes));

                    //Environment.Exit(0);

                    throw new Exception(string.Format("The DIU Process Failed. Please check the CRM for the error details. Import Job Name = {0}, ProcessID= {1}", importJob.Name, processId));
                }
            }

            catch (Exception ex)
            {
                BEMLogger.LogMessage("Error in DIU Task", ex, Level.Error, ApplicationFriendlyName, logger);
                throw ex;
            }

            BEMLogger.LogMessage("Completed DIU Task. ", null, Level.Info, ApplicationFriendlyName, logger);
            return processId;

        }

        private static IList<DataImportFileSheetDto> ApplyTemplate(IList<DataImportFileSheetDto> filesheets)
        {
            try
            {
                foreach (DataImportFileSheetDto fileSheetDto in filesheets)
                {
                    fileSheetDto.Id = 0;
                    foreach (var map in fileSheetDto.Mappings)
                    {
                        map.Id = 0;
                    }
                }
            }
            catch (Exception ex)
            {
                BEMLogger.LogMessage("Error in ApplyTemplate ", ex, Level.Error, ApplicationFriendlyName, logger);
            }
            return filesheets;
        }

        #endregion

    }
    #endregion

    #region CompleteBatchTask

    [DisallowConcurrentExecution]
    public class CompleteBatchTask : BEMBaseTask, IJob
    {
        #region Execute

        public void Execute(IJobExecutionContext context)
        {

            try
            {
                #region Execute

                int _eventCounter = 0;


                foreach (string _objectId in ObjectIDs.Split(','))
                {
                    _eventCounter++;

                    SqlCommand _command = new SqlCommand(@"[JOB].[spCompleteBatchEvent]");
                    _command.Parameters.Add(new SqlParameter("@ObjectID", Int32.Parse(_objectId)));
                    _command.Parameters.Add(new SqlParameter("@diuProcessId", DIUProcessID));
                    _command.Parameters.Add(new SqlParameter("@IsFinalEvent", (_eventCounter == ObjectIDs.Split(',').Length ? 1 : 0)));
                    _command.CommandType = System.Data.CommandType.StoredProcedure;
                    _command.CommandTimeout = 1800;
                    var result = DatabaseCalls.ExecuteNonQuery(_command, "bemstg");

                }
                #endregion
            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }

        }

        #endregion
    }

    #endregion

    #region LoadMarketDataTask
    [DisallowConcurrentExecution]
    public class LoadMarketDataTask : BEMBaseTask, IJob
    {

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                DIUTemplateName = ConfigurationManager.AppSettings.Get("DIUMarketDataTemplate");
                SSRSReportName = ConfigurationManager.AppSettings.Get("SSRSMarketDataReport");
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region CompleteMarketDataEventJob
    [DisallowConcurrentExecution]
    public class CompleteMarketDataEventJob : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {

            try
            {

                int diuProcessID = Int32.Parse(DIUProcessID);

                if (diuProcessID == 0)
                {
                    BEMLogger.LogMessage("Process ID Not found.", null, Level.Error, ApplicationFriendlyName, logger);
                    throw new Exception("Process ID Not found.");
                }

                SqlCommand _command = new SqlCommand();
                _command.CommandText = "[MARKET].[Exchange_Rate_ChangeStatusCode]";
                _command.CommandType = CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;
                _command.Parameters.Add(new SqlParameter("@ProcessID", diuProcessID));

                using (SqlConnection _connection = new SqlConnection(ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                {
                    _connection.Open();
                    _command.Connection = _connection;
                    _command.ExecuteNonQuery();
                }
            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }

        }
    }

    #endregion

    #region NightlyFeedReferentialJob
    [DisallowConcurrentExecution]
    public class NightlyFeedReferentialJob : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                DIUTemplateName = ConfigurationManager.AppSettings.Get("DIUNightlyFeedReferentialTemplate");
                SSRSReportName = ConfigurationManager.AppSettings.Get("SSRSNightlyFeedReferentialReport");
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }

    #endregion

    #region CompleteNightlyFeedReferentialDataTask
    [DisallowConcurrentExecution]
    public class CompleteNightlyFeedReferentialDataTask : BEMBaseTask, IJob
    {

        #region Public Methods
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                SqlCommand _command = new SqlCommand();
                _command.CommandText = "[JOB].[spSendNightlyFeedDataLoadConfirmation]";
                _command.CommandType = CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;
                _command.Parameters.Add(new SqlParameter("@ProcessID", DIUProcessID));
                _command.Parameters.Add(new SqlParameter("@JobrequestID", JobRequestId));

                using (SqlConnection _connection = new SqlConnection(ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                {
                    _connection.Open();
                    _command.Connection = _connection;
                    _command.ExecuteNonQuery();
                }
            }

            catch (Exception ex)
            {
                throw new JobExecutionException(ex.Message, ex);
            }

        }
        #endregion
    }

    #endregion

    #region SendEmailTask
    [DisallowConcurrentExecution]
    public class SendEmailTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                string emailSvcProvider = BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("SystemDefaultEmailServiceProvider").Value;
                string appEnvironmentName = BEMContainer.Current.Resolve<IBemLookupService<ConfigurationEntry>>().FindByName("AppEnvironmentName").Value;

                BEMLogger.LogMessage(string.Format("Email Service Provicer = {0}, AppEnvironmentName = {1} ", emailSvcProvider, appEnvironmentName), Level.Debug, ApplicationFriendlyName, logger);


                if (emailSvcProvider == "DBEmailService")
                {
                    #region DB Email
                    using (SqlConnection scon = new SqlConnection(
                            ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                    {
                        SqlCommand cmd = new SqlCommand();
                        cmd.Connection = scon;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 1800;
                        cmd.CommandText = "Job.SendEmail";
                        cmd.Parameters.Add(
                            new SqlParameter("@JobRequestId", this.JobRequestId)
                            );
                        scon.Open();
                        cmd.ExecuteNonQuery();
                    }
                    #endregion
                }


                if (emailSvcProvider == "EmailWebService")
                {
                    #region Email Web Service

                    SecurityContext contextSecure;
                    System.Net.CookieContainer cookieContainerdata;
                    System.Net.CookieCollection cookiecoll;
                    SendMailService serviceObject = null;
                    string[] emailImages = new string[1];
                    IList<string> fileNames = new List<string>();

                    serviceObject = new SendMailService();
                    serviceObject.Credentials = System.Net.CredentialCache.DefaultCredentials;
                    serviceObject.Url = System.Configuration.ConfigurationManager.AppSettings.Get("EmailServiceUrl");
                    serviceObject.PreAuthenticate = true;

                    contextSecure = new SecurityContext();
                    contextSecure.userId = System.Configuration.ConfigurationManager.AppSettings.Get("BEMUser");
                    contextSecure.password = Helper.DecryptPassword(System.Configuration.ConfigurationManager.AppSettings.Get("BEMPwd"));

                    serviceObject.SecurityContextValue = contextSecure;

                    cookieContainerdata = new System.Net.CookieContainer();
                    serviceObject.CookieContainer = cookieContainerdata;
                    cookiecoll = new System.Net.CookieCollection();


                    if (serviceObject.LoginUser())
                    {
                        System.Net.Cookie sessioncookie = new System.Net.Cookie();

                        cookiecoll = serviceObject.CookieContainer.GetCookies(new Uri(serviceObject.Url));
                        sessioncookie = cookiecoll[System.Configuration.ConfigurationManager.AppSettings.Get("CookieName")];
                        if ((sessioncookie != null))
                        {
                            serviceObject.CookieContainer.Add(sessioncookie);
                        }
                        var emailRequestQuery = NHSessionManager.GetCurrentStagingSession(Utils.CurrentDBEnvironmentName).Query<EmailRequest>();

                        var emailRequests = emailRequestQuery.Where(er => er.JobRequestId == this.JobRequestId);
                        foreach (var emailRequest in emailRequests)
                        {
                            string toList = string.Empty;
                            string ccList = string.Empty;
                            string bccList = string.Empty;
                            try
                            {
                                BEMLogger.LogMessage(string.Format("Passed Values : ToList={0}, CcList={1}, BccList={2}, Subject={3}", emailRequest.ToList, emailRequest.CcList, emailRequest.BccList, emailRequest.Subject), Level.Debug, ApplicationFriendlyName, logger);
                                toList = emailRequest.ToList.Replace(';', ',').Replace(",,", ",").Replace(",,", ",").Replace(",,", ",");
                                ccList = emailRequest.CcList.Replace(';', ',').Replace(",,", ",").Replace(",,", ",").Replace(",,", ",");
                                bccList = emailRequest.BccList.Replace(';', ',').Replace(",,", ",").Replace(",,", ",").Replace(",,", ",");

                                toList = toList.TrimEnd(' ').TrimEnd(',');
                                ccList = ccList.TrimEnd(' ').TrimEnd(',');
                                ccList = ccList.TrimEnd(' ').TrimEnd(',');

                                BEMLogger.LogMessage(string.Format("After parsing: ToList={0}, CcList={1}, BccList={2}, Subject={3}", toList, ccList, bccList, emailRequest.Subject), Level.Debug, ApplicationFriendlyName, logger);
                            }
                            catch {
                                BEMLogger.LogMessage(string.Format("Error in parsing the email ids"), Level.Debug, ApplicationFriendlyName, logger);
                            }

                            #region Email With No Attachments
                            if (emailRequest.AttachmentsList == null || emailRequest.AttachmentsList == string.Empty)
                            {
                                serviceObject.SendMail
                                    (
                                        toList,
                                        ccList,
                                        bccList,
                                        "Investran.Support@ifc.org",
                                        emailRequest.Subject + " " + appEnvironmentName,
                                        emailRequest.Body,
                                        emailImages
                                    );
                            }
                            #endregion

                            #region Email with Attachments
                            if (emailRequest.AttachmentsList != string.Empty)
                            {
                                IList<byte[]> fileAttachments = new List<byte[]>();

                                foreach (var fileattchment in emailRequest.AttachmentsList.Split('|'))
                                {
                                    fileAttachments.Add(File.ReadAllBytes(fileattchment));
                                    fileNames.Add(Path.GetFileName(fileattchment));
                                }

                                serviceObject.SendMailWithAttachments(
                                    toList,
                                    ccList,
                                    bccList,
                                    fromList: "Investran.Support@ifc.org",
                                    subject: emailRequest.Subject + " " + appEnvironmentName,
                                    body: emailRequest.Body,
                                    emailImages: null,
                                    attachmentFiles: fileAttachments.ToArray(),
                                    attachmentFileNames: fileNames.ToArray()
                                    );

                            }
                            #endregion
                        }

                    }
                }
                    #endregion
            }

            catch (Exception exception)
            {
                // Do not throw Job Execution Exception. Email Failure shouldn't fail the job.
                BEMLogger.LogMessage(exception.Message, exception, Level.Critical, ApplicationFriendlyName, logger);
                //throw new JobExecutionException(exception.Message, exception);

            }
        }

    }
    #endregion

    #region ExecuteSqlRequestsTask
    [DisallowConcurrentExecution]
    public class ExecuteSqlRequestsTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                using (SqlConnection scon = new SqlConnection(
                        ConfigurationManager.ConnectionStrings["bemstg"].ToString()))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = scon;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 7200; //2 hrs ... why ?
                    cmd.CommandText = "[JOB].[spExecuteSqlRequests]";
                    cmd.Parameters.Add(
                        new SqlParameter("@JobRequestId", this.JobRequestId)
                        );
                    scon.Open();
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }

    }
    #endregion

    #region ExecuteSqlTask
    //[DisallowConcurrentExecution] -- This task should be allowed to be executed in parallel (i.e, multiple instances)
    public class ExecuteSqlTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                string dbProcHandler = string.Empty;
                string sqlStatement = string.Empty;
                string connection = string.Empty;

                var map = context.MergedJobDataMap;
                if (map["DbProcHandler"] == null || map["DbProcHandler"].ToString() == string.Empty)
                {
                    throw new Exception(string.Format("The DbProcName is missing for the TaskName {0}", map["TaskName"]));
                }

                sqlStatement = map["DbProcHandler"].ToString().Split(',')[0];
                connection = map["DbProcHandler"].ToString().Split(',')[1];


                var contextVariables = DatabaseCalls.GetJobExecutionContextVariables(JobRequestId);

                SqlCommand _command = new SqlCommand(sqlStatement);
                _command.CommandType = System.Data.CommandType.StoredProcedure;
                _command.CommandTimeout = 1800;


                using (SqlConnection _connection = new SqlConnection(ConfigurationManager.ConnectionStrings[connection].ToString()))
                {
                    _connection.Open();
                    _command.Connection = _connection;

                    SqlCommandBuilder.DeriveParameters(_command);
                    foreach (SqlParameter param in _command.Parameters)
                    {
                        if (param.Direction != ParameterDirection.Input)
                            continue;

                        string selectExpression = string.Format("ParameterName = '{0}'", param.ParameterName.Replace("@", ""));
                        var selecteRows = contextVariables.Select(selectExpression);
                        if (selecteRows.Length == 1)
                        {
                            param.Value = selecteRows[0]["ParameterValue"].ToString();
                        }
                        else
                        {
                            param.Value = DBNull.Value;
                            //throw new Exception(string.Format("The required parameter {0} for the Procedure {1} is not found in the Execution Context. Note that Parameter matching is Case-Sensitive.", param.ParameterName, sqlStatement));
                        }
                    }
                    //var resultInt = _command.ExecuteNonQuery();
                }

                var resultFromDB = DatabaseCalls.GetDataTable(_command, connection);
                if (resultFromDB != null)
                {
                    this.DataTable = Helper.GetXml(resultFromDB);
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region ExecuteSqlTextTask
    //[DisallowConcurrentExecution] -- This task should be allowed to be executed in parallel (i.e, multiple instances)
    public class ExecuteSqlTextTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                string dbProcHandler = string.Empty;
                string sqlStatement = string.Empty;
                string connection = string.Empty;

                var map = context.MergedJobDataMap;
                if (map["DbProcHandler"] == null || map["DbProcHandler"].ToString() == string.Empty)
                {
                    throw new Exception(string.Format("The DbProcName is missing for the TaskName {0}", map["TaskName"]));
                }

                sqlStatement = map["DbProcHandler"].ToString().Split('|')[0];
                connection = map["DbProcHandler"].ToString().Split('|')[1];

                SqlCommand _command = new SqlCommand(sqlStatement);
                _command.CommandType = System.Data.CommandType.Text;
                _command.CommandTimeout = 1800;

                var resultFromDB = DatabaseCalls.GetDataTable(_command, connection);
                if (resultFromDB != null)
                {
                    this.DataTable = Helper.GetXml(resultFromDB);
                    this.RecordCount = resultFromDB.Rows.Count.ToString();
                }
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region SendFileOverFTPTask
    /// <summary>
    /// FTPs a file based on the FTPProfile Name (JOB.FTPProfiles table)
    /// </summary>
    public class SendFileOverFTPTask : BEMBaseTask, IJob
    {
        private TransferOptions transferOptions;

        public void Execute(IJobExecutionContext context)
        {

            var ftpProfile = NHSessionManager.GetCurrentStagingSession(Utils.CurrentDBEnvironmentName).Query<FTPProfile>().Where(prf => prf.ProfileName == this.FTPProfileName).SingleOrDefault();

            SessionOptions sessionOptions = new SessionOptions()
            {
                Protocol = Protocol.Sftp,
                HostName = ftpProfile.TargetServer,
                UserName = ftpProfile.UserName,
                SshPrivateKeyPath = ConfigurationManager.AppSettings.Get("SFTPPPKFile"),
                SshHostKeyFingerprint = ftpProfile.SshHostKeyFingerprint//@"ssh-ed25519 256 28:f0:5c:b7:69:b7:9d:90:9f:fc:c0:23:e5:90:0c:96"
            };

            try
            {
                using (Session session = new Session())
                {
                    // Connect
                    session.Open(sessionOptions);

                    // Upload files
                    transferOptions = new TransferOptions();
                    transferOptions.TransferMode = TransferMode.Binary;

                    TransferOperationResult transferResult;
                    string remotePath = string.Format("{0}/{1}", ftpProfile.TargetPath, Path.GetFileName(this.FTPFilePath));
                    transferResult =
                        session.PutFiles(this.FTPFilePath, remotePath, false, transferOptions);

                    // Throw on any error
                    transferResult.Check();

                    foreach (TransferEventArgs transfer in transferResult.Transfers)
                    {
                        BEMLogger.LogMessage(string.Format("Upload of {0} succeeded", transfer.FileName), Level.Info, ApplicationFriendlyName, logger);
                    }
                }
            }
            catch (Exception exception)
            {

                throw new JobExecutionException(exception.Message, exception);
            }

        }
    }


    #endregion

    #region PrepareSAPFTPTask
    /// <summary>
    /// It's a SAP Job Specific Task. 
    /// Renames the generated text file to SAP Specific File Name and makes it available for FTP and Email
    /// </summary>
    public class PrepareSAPFTPTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                string _newFilePath = string.Empty;

                _newFilePath = string.Format(@"{0}\{1}", Path.GetDirectoryName(this.FilePath), this.SAPFileName);

                File.Move(this.FilePath, _newFilePath);

                this.FTPFilePath = _newFilePath;
                this.EmailAttachmentFilePath = _newFilePath;
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }

        }
    }

    #endregion

    #region CreateDelimitedFileFromDataTableTask
    /// <summary>
    /// Creates a delimited Text file from the DataTable that is available in Contexct Variables
    /// </summary>
    public class CreateDelimitedFileFromDataTableTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {

                DataTable _dataTable = null;
                if (this.DataTable != null)
                {
                    _dataTable = this.DataTable.XmlDeserializeFromString<System.Data.DataTable>();
                }
                else
                {
                    throw new JobExecutionException("DataTable Param does not exists in the Context Variables");
                }

                var fileName = string.Format("JobRequestID_{0}_{1}.txt", JobRequestId, DateTime.Now.ToString("MM-dd-yyyy_hh-mm-ss-ms"));
                var filePath = Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory) + "\\DBResultFiles\\" + fileName;
                BEMLogger.LogMessage(string.Format("File Path ={0}", filePath), Level.Info, ApplicationFriendlyName, logger);

                List<string> fileContent = _dataTable.ConvertDataTableToDelimitedText("", false, false);

                using (StreamWriter sw = File.CreateText(filePath))
                {
                    //sw.Write(fileContent);
                    foreach (var line in fileContent)
                    {
                        sw.WriteLine(line);
                    }
                }

                this.FilePath = filePath;
            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #region CreateDelimitedFileFromDataTableTask
    /// <summary>
    /// Creates a delimited Text file from the DataTable that is available in Contexct Variables
    /// </summary>
    public class CreateHtmlTableFromDataTableTask : BEMBaseTask, IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {

                DataTable _dataTable = null;
                if (this.DataTable != null)
                {
                    _dataTable = this.DataTable.XmlDeserializeFromString<System.Data.DataTable>();
                }
                else
                {
                    throw new JobExecutionException("DataTable Param does not exists in the Context Variables");
                }

                this.HtmlTable = _dataTable.ConvertDataTableToHTML();

            }
            catch (Exception exception)
            {
                throw new JobExecutionException(exception.Message, exception);
            }
        }
    }
    #endregion

    #endregion

}