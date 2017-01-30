#!/home/galaxy/galaxy/.venv/bin/python
"""Offload jobs to a Mesos cluster.
"""


import logging
import httplib2
import json

import time
import inspect
import requests
from datetime import datetime


from galaxy import model
from galaxy.jobs.runners import AsynchronousJobState, AsynchronousJobRunner
from os import environ as os_environ
from six import text_type
from requests import Request, Session

from base64 import b64encode



# Chronos package imports:
try:
    import chronos     

except ImportError as exc:
    chronos = None
    GOCHRONOS_IMPORT_MESSAGE = ('The Python Chronos Client package is required to use '
                          'this feature, please install it or correct the '
                          'following error:\nImportError %s' % str(exc))

class ChronosAPIError(Exception):
     pass

log = logging.getLogger(__name__)

__all__ = ['GoChronosJobRunner']

gochronos_exit_job_code={}

class ChronosClient(object):
    _user = None
    _password = None

    def __init__(self, servers, proto="https", username=None, password=None, level='WARN'):
        #server_list = servers if isinstance(servers, list) else [servers]
        #self.servers = ["%s://%s" % (proto, server) for server in server_list]
        self.servers = ["https://172.30.67.7:4443"]
        log.debug( "SELF SERVER IS: %s" % self.servers) 
        if username and password:
            self._user = username
            self._password = password
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=level)
        self.logger = logging.getLogger(__name__)

    def _list_all(self):
        """List all jobs on Chronos."""
        return self._call("/scheduler/jobs", "GET")

    def _list(self,job_name):
        """List all jobs on Chronos."""
        return self._call("/scheduler/jobs/search?name="+job_name, "GET")

    def _list_jobs_graph(self):
        """ Send command /scheduler/graph/csv  and  returns  data  as 
             node,myjob1,fresh,running
             node,myjob2,failure,idle """
        return self._call('/scheduler/graph/csv', "GET")


    def get(self,name):
        """List name job on Chronos."""
        path = "/scheduler/job/%s" % name
        return self._call(path, "GET")

    def delete(self, name):
        """Delete a job by name"""
        path = "/scheduler/job/%s" % name
        return self._call(path, "DELETE")

    def delete_tasks(self, name):
        """Terminate all tasks for a running/stuck job"""
        path = "/scheduler/task/kill/%s" % name
        return self._call(path, "DELETE")

    def run(self, name):
        log.debug("Run a job by name")
        path = "/scheduler/job/%s" % name
        return self._call(path, "PUT")

    def add(self, job_def, update=False):
        log.debug("Schedule a new job")
        path = "/scheduler/iso8601"
        self._check_fields(job_def)
        if "parents" in job_def:
            path = "/scheduler/dependency"
        if update:
            method = "PUT"
        else:
            method = "POST"
        return self._call(path, method, json.dumps(job_def))
        

    def update(self, job_def):
        """Update an existing job by name"""
        return self.add(job_def, update=True)

    def job_stat(self, name):
        """ List stats for a job """
        return self._call('/scheduler/job/stat/%s' % name, "GET")

    def scheduler_stat_99th(self):
        return self._call('/scheduler/stats/99thPercentile', 'GET')

    def scheduler_stat_98th(self):
        return self._call('/scheduler/stats/98thPercentile', 'GET')

    def scheduler_stat_95th(self):
        return self._call('/scheduler/stats/95thPercentile', 'GET')

    def scheduler_stat_75th(self):
        return self._call('/scheduler/stats/75thPercentile', 'GET')

    def scheduler_stat_median(self):
        return self._call('/scheduler/stats/median', 'GET')

    def scheduler_stat_mean(self):
        return self._call('/scheduler/stats/mean', 'GET')
    

    def _call(self, url, method="GET", body=None, headers={}):
        hdrs = {}
        if body:
            hdrs['Content-Type'] = "application/json"
        hdrs.update(headers)
        log.debug("Fetch: %s %s" % (method, url))
        if body:
            log.debug("Body: %s" % body)
        conn = httplib2.Http(disable_ssl_certificate_validation=True)
        if self._user and self._password:
            conn.add_credentials(self._user, self._password)

        response = None
        servers = list(self.servers)
        while servers:
            server = servers.pop(0)
            endpoint = "%s%s" % (server, url)
            try:
                resp, content = conn.request(endpoint, method, body=body, headers=hdrs)
            except (socket.error, httplib2.ServerNotFoundError) as e:
                log.debug('Error while calling %s: %s. Retrying', endpoint, e.message)
                continue
            try:
                response = self._check(resp, content)
                return response
            except ChronosAPIError as e:
                log.debug('Error while calling %s: %s', endpoint, e.message)

        raise ChronosAPIError('No remaining Chronos servers to try')
  

    def _check(self, resp, content):
        status = resp.status
        log.debug("status: %d" % status)
        #self.logger.debug("status: %d" % status)
        payload = None

        if status == 401:
            raise UnauthorizedError()

        if content:
            try:
                payload = json.loads(content)
            except ValueError:
                log.debug("Response not valid json: %s" % content)
                #self.logger.error("Response not valid json: %s" % content)
                payload = content

        if payload is None and status != 204:
            raise ChronosAPIError("Request to Chronos API failed: status: %d, response: %s" % (status, content))

        return payload

    def _update(self, result, job):
        job['result_fields']['status'] = result.status
        self.logger.debug("status: %d" % status)
        payload = None


        if result.content:
            try:
                payload = json.loads(result.content)
            except ValueError:
                self.logger.error("Response not valid json: %s" % content)
                payload = content

        if payload is None:  
              job['result_fields']['content']=payload 
        return True


    def _check_fields(self, job):
        log.debug( "check fields for CHRONOS JOB")
        for k in ChronosJob.fields:
            if k not in job:
                raise MissingFieldError("missing required field %s" % k)
        
        if any(field in job for field in ChronosJob.one_of):
            if len([field for field in ChronosJob.one_of if field in job]) > 1:
                raise OneOfViolationError("Job must only include 1 of %s" % ChronosJob.one_of)
        else:
            raise MissingFieldError("Job must include one of %s" % ChronosJob.one_of)

        if "container" in job:
            container = job["container"]
            for k in ChronosJob.container_fields:
                if k not in container:
                    raise MissingFieldError("missing required container field %s" % k)

        return True

class ChronosJob(object):
    fields = [
        "async",
        "command",
        "epsilon",
        "name",
        "owner"
    ]
    one_of = ["schedule", "parents"]
    container_fields = [
        "type",
        "image"
    ]
    result_fields = [
     "status",
     "content" 
    ]


class GoChronosJobRunner(AsynchronousJobRunner):
    """Job runner backed by a finite pool of worker threads. FIFO scheduling
    """
    runner_name = "GoChronosRunner"

    def __init__(self, app, nworkers, **kwargs):
        assert chronos is not None, GOCHRONOS_IMPORT_MESSAGE
        log.debug("Loading app %s", app)
        runner_param_specs = dict(chronos_server=dict(map=str),user=dict(map=str), password=dict(map=str))
            
        if 'runner_param_specs' not in kwargs:
            kwargs['runner_param_specs'] = dict()
        kwargs['runner_param_specs'].update(runner_param_specs)

        """Start the job runner parent object """
        super(GoChronosJobRunner, self).__init__(app, nworkers, **kwargs)
        
        log.debug("Creo l'oggetto Chronos client")
        
        self.chronos_cli = ChronosClient(self.runner_params["chronos_server"], proto="https", username=self.runner_params["user"],
                           password=self.runner_params["password"])
        
        
        if not self.chronos_cli:
            log.debug("Connection failure!! Runner cannot be started")
        else:
            
            self._init_monitor_thread()
            self._init_worker_threads()
       

    def queue_job(self, job_wrapper):
        """Create Chronos job and submit it to Mesos cluster"""
        # prepare the job
        # We currently don't need to include_metadata or include_work_dir_outputs, as working directory is the same
        # were galaxy will expect results.
        log.debug("Starting queue_job for job " + job_wrapper.get_id_tag())
        if not self.prepare_job(job_wrapper, include_metadata=False, include_work_dir_outputs=False):
            return

        job_destination = job_wrapper.job_destination
        job_id = self.post_task(job_wrapper)
        if not job_id:
            log.debug("Job creation failure!! No Response from GoChronos")
            job_wrapper.fail("Not submitted")
        else:
            log.debug("ChronosTask created " + job_id)
            # Create an object of AsynchronousJobState and add it to the monitor queue.
            ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper, job_id=job_id, job_destination=job_destination)
            self.monitor_queue.put(ajs)
        return None 

    def connect(servers, proto="https", username="admin", password=None):
        return ChronosClient(servers, proto=proto, username=username, password=password)

    def __produce_gochronos_job_name(self, job_id):
        # wrapper.get_id_tag() instead of job_id for compatibility with TaskWrappers.
        return "ChronosTask_" + str(job_id)

    def post_task(self, job_wrapper):
        """ Sumbit job to Mesos cluster and return jobid
            Create Job model schema of GoChronos and call the http_post_request method.
        """
        # Get the params from <destination> tag in job_conf by using job_destination.params[param]
        if self.chronos_cli:
          log.debug(" CHRONOS CLI esisteee!\n")
        job_destination = job_wrapper.job_destination
        try:
           mesos_task_cpu = int(job_destination.params["mesos_task_cpu"])
           log.debug("Mesos_task_cpu OK")
        except:
           mesos_task_cpu = 0.1
        try:
           mesos_task_disk = int(job_destination.params["mesos_task_disk"])
           log.debug("Mesos_task_disk OK")
        except:
           mesos_task_disk = 256 #MB
        try:
           mesos_task_mem = int(job_destination.params["mesos_task_mem"])
        except:
           mesos_task_mem = 128
        try:
           workingDirectory=job_wrapper.working_directory
           job_tool=job_wrapper.tool
           docker_image = self._find_container(job_wrapper).container_id
           log.debug("DOCKER IMAGE: %s \n",docker_image)
           log.debug("Job tool: %s",job_tool)
           log.debug("work dir: %s",workingDirectory)
        except:
           log.debug("Docker_image not specified in Job config and Tool config!!")
           
           """try:
                log.debug(self.runner_params["gomesos_docker_project"])
                project = str(self.runner_params["gomesos_docker_project"])
            except KeyError:
                log.debug("gomesosdocker_project not defined, using defaults")
           """
        volumes = []
        try:
            if (job_destination.params["gochronos_volumes_containerPath"]):
             volume = job_destination.params["gochronos_volumes_containerPath"]
             volume = volume.split(",")
             log.debug("VOLUME is s%", volume)
             for i in volume:
                temp = dict({"containerPath":job_destination.params["gochronos_volumes_containerPath"],              
                             "hostPath":job_destination.params["gochronos_volumes_hostPath"],"mode":"RW"})
                volumes.append(temp)
        except:
                log.debug("gochronos_volumes not set. Getting default volume!!")

        try:
            if (job_destination.params["gochronos_volumes_containerPath"]):
                    src_command = "/bin/bash " + job_wrapper.runner_command_line
                    command = src_command.replace(volumes[0]["hostPath"],volumes[0]["containerPath"])
                    log.debug("NEW COMMAND IS %s",command)
            else:        
               command = "/bin/bash " + job_wrapper.runner_command_line
        except:
               command = "/bin/bash " + job_wrapper.runner_command_line
        
        
        gochronos_jobname=self.__produce_gochronos_job_name(job_wrapper.job_id)
        gochronos_job = {
              "name": gochronos_jobname,
              "command": command,
              "schedule":"R1//P10M", 
              "scheduleTimeZone":"LMT",
              "epsilon": "PT60S",
              "owner": None,
              "shell":True,
              "async":False,
              "container": {
                "type": "DOCKER",
                "image":job_destination.params["gochronos_default_container_id"], # self._find_container(job_wrapper).container_id,
                "volumes": volumes
              },
              "successCount": 0,
              "errorCount": 0, 
              "cpus": mesos_task_cpu,
              "mem": mesos_task_mem,
              "disk":mesos_task_disk,
              #"fetch": [{ "uri":"file:///home/galaxy/galaxy/database/files/000/dataset_95.dat"}], #,"https://example.com/app/cool-script.sh",
                        #"https://example.com/app.zip", o un .json, un jpg,etc... 
              "dataJob": False,
              "environmentVariables":[],
              "constraints":[]
            }
         
        log.debug("\n JOB POST TASK TO BE EXECUTED \n")
        result = self.chronos_cli.add(gochronos_job)
                
        log.debug("Response from gochronos task :  %s" % result)
        #gochronos_exit_job_code[gochronos_jobname]=result['status']  
           
        return gochronos_jobname
    

    ''' get  possible job states
    CHRONOS job states: idle,running,queued,failed,started,finished,disabled,skipped,expired,removed
    (TODO: check if there are more possible CHRONOS job states)
    '''
    def _get_chronos_job_state(self, job_id):
        '''Given a job id, calls Chronos to know the state of that job'''
        chronos_jobs = self.chronos_cli._list_jobs_graph()
        if chronos_jobs:
            parsed_chronos_jobs = chronos_jobs.split("\n")
            for chronos_job in parsed_chronos_jobs:
                if chronos_job != '':
                    properties = chronos_job.split(",")
                    # properties[1] --> Job name
                    if job_id == properties[1]:
                        log.debug("TROVATO %s", job_id)# properties[3] --> Job state
                        return properties[3]
    
    def create_log_file(self, job_state,status):
        """ Create log files in galaxy, namely error_file, output_file, exit_code_file
            Return true, if all the file creations are successful
        """
        path = "/var/log"
        chroj_output_file = path + "/chroj.log"
        chroj_error_file = path + "/chroj.err"
        try:
                # Read from GoChronos output_file and write it into galaxy output_file.
                f = open(chroj_output_file, "r")
                out_log = f.read()
                log_file = open(job_state.output_file, "w")
                log_file.write(out_log)
                log_file.close()
                f.close()
                # Read from GoChronos error_file and write it into galaxy error_file.
                f = open(chroj_error_file, "r")
                out_log = f.read()
                log_file = open(job_state.error_file, "w")
                log_file.write(out_log)
                log_file.close()
                f.close()
                # Read from GoMesos exit_code and write it into galaxy exit_code_file.
                out_log = gochronos_exit_job_code[job_state.job_id]  #204/500/etc..DA VEDERE COME PRENDERLI!
                log_file = open(job_state.exit_code_file, "w")
                log_file.write(out_log)
                log_file.close()
                f.close()
                log.debug("CREATE OUTPUT FILE: " + str(job_state.output_file))
                log.debug("CREATE ERROR FILE: " + str(job_state.error_file))
                log.debug("CREATE EXIT CODE FILE: " + str(job_state.exit_code_file))
        except IOError as e:
                log.error('Could not access task log file %s' % str(e))
                log.debug("IO Error occurred when accessing the files!!")
                return False
        return True

   ###############################################################################################################
   # [{"name":"SAMPLE_JOB1","command":"echo 'ROXANNE ROXANNE ROXANNE'","shell":true,"epsilon":"PT60S","executor":"","executorFlags":"",
   #"taskInfoData":"","retries":2,"owner":"gallitelli@live.com",
   #"ownerName":"","description":"","async":true,"successCount":0,"errorCount":0,"lastSuccess":"","lastError":"","cpus":0.1,
   #"disk":256.0,"mem":128.0,"disabled":false,"softError":false,"dataProcessingJobType":false,"errorsSinceLastSuccess":0,
   #"fetch":   [],"uris":[],"environmentVariables":[],"arguments":[],"highPriority":false,"runAsUser":"root","constraints":[],"schedule":
   #"R1//PT10M","scheduleTimeZone":""},{"name":"test","command":
   #"echo hello","shell":true,"epsilon":"PT30M","executor":"","executorFlags":"",
   #"taskInfoData":"","retries":2,"owner":"marica.antonacci@gmail.com",
#"ownerName":"","description":"","async":false,"successCount":1,"errorCount":0,"lastSuccess":"2016-11-15T09:51:39.560Z","lastError":"",
#"cpus":0.1,"disk":256.0,"mem":128.0,"disabled":true,"softError":false,"dataProcessingJobType":false,"errorsSinceLastSuccess":0,"fetch":[],"uris":[],"environmentVariables":[],"arguments":[],"highPriority":false,"runAsUser":"root","constraints":[],"schedule":"R0/2016-11-16T09:51:17.000Z/PT24H","scheduleTimeZone":""}](.venv)galaxy@galaxy:~$ 


  ###############################################################################################
    def check_watched_item(self, job_state):
        log.debug("Checks the state of a job already submitted on GoChronos. Job state is a AsynchronousJobState\n")
        log.debug("job_state.job_wrapper.job_id %d" % job_state.job_wrapper.job_id)
        gomesos_job_name=self.__produce_gochronos_job_name(job_state.job_wrapper.job_id)
        response=self.chronos_cli._list(gomesos_job_name)
         
        
        succeeded=0
        failed=0
        if len(response) == 1:
            if response[0]['successCount']>=1: 
                    job_state.job_wrapper.change_state(model.Job.states.OK)
                    succeeded = response[0]['successCount']
            if response[0]['errorCount']>=1: 
                    job_state.job_wrapper.change_state(model.Job.states.ERROR)
                    failed = response[0]['errorCount']
            if succeeded:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.OK)
                job_chronos_status="OK"
                #self.create_log_file(job_state, job_chronos_status)
                self.mark_as_finished(job_state)
                return None
            elif failed:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.ERROR)
                job_chronos_status="ERROR"
                #self.create_log_file(job_state, job_chronos_status)
                self.mark_as_failed(job_state)
                return None
            elif self._get_chronos_job_state(gomesos_job_name) == "running":
                job_state.running = True
                job_state.job_wrapper.change_state(model.Job.states.RUNNING)
                return job_state

            elif self._get_chronos_job_state(gomesos_job_name) == "queued":
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.QUEUED)
                #log.debug("SONO IN CODA!")
                return job_state


        elif len(response) == 0:
            # there is no job responding to this job_id, it is either lost or something happened.
            #self.create_log_file(job_state,"ERROR")
            self.mark_as_failed(job_state)
            return job_state
        else:
            # there is more than one job associated to the expected unique job id used as selector.
            log.error("There is more than one Chronos Job associated to job id " + job_state.job_id)
            job_state.job_wrapper.change_state(model.Job.states.ERROR)
            #self.create_log_file(job_state,"ERROR")
            self.mark_as_failed(job_state)
            return job_state

        """ def fail_job(self, job_state):  NON E' DETTO CHE SERVA SOVRASCRIVERLA
         """
    def stop_job(self, job):
        """Attempts to delete a dispatched job to the mesos cluster"""
        try:
            job_name=self.__produce_gochronos_job_name(job.id)
            log.debug("STOP JOB EXECUTION OF JOB ID: " + job_name)
            self.chronos_cli.delete(job_name) 
            
            # TODO assert whether job parallelism == 0
            # assert not job_to_delete.exists(), "Could not delete job,"+job.job_runner_external_id+" it still exists"
            log.debug("(%s) Terminated at user's request" % (job.id))
        except Exception as e:
            log.debug("(%s) User killed running job, but error encountered during termination NO JOB STOPPED: %s" % (
                job.id,e))

    def recover(self, job, job_wrapper):
        """Recovers jobs stuck in the queued/running state when Galaxy started"""
        # TODO this needs to be implemented to override unimplemented base method
        job_id = job_wrapper.job_id  
        ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper)
        ajs.job_id = str(job_id)
        ajs.command_line = job.command_line
        ajs.job_wrapper = job_wrapper
        ajs.job_destination = job_wrapper.job_destination
        if job.state == model.Job.states.RUNNING:
            log.debug("(%s/%s) is still in running state, adding to the runner monitor queue" % (
                job.id, job_id))
            ajs.old_state = model.Job.states.RUNNING
            ajs.running = True
            self.monitor_queue.put(ajs)
        elif job.state == model.Job.states.QUEUED:
            log.debug("(%s/%s) is still in queued state, adding to the runner monitor queue" % (
                job.id, job_id))
            ajs.old_state = model.Job.states.QUEUED
            ajs.running = False
            self.monitor_queue.put(ajs)
        

   
     

