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
    PYMESOS_IMPORT_MESSAGE = ('The Python Chronos Client package is required to use '
                          'this feature, please install it or correct the '
                          'following error:\nImportError %s' % str(exc))

class ChronosAPIError(Exception):
     pass

class UnauthorizedError(Exception):
     pass


log = logging.getLogger(__name__)

__all__ = ['PyMesosJobRunner']



class ChronosClient(object):
    _user = None
    _password = None

    def __init__(self, servers, username=None, password=None, level='WARN'):
        #server_list = servers if isinstance(servers, list) else [servers]
        #self.servers = ["%s://%s" % (proto, server) for server in server_list]
        self.chronos = "https://172.30.67.7:4443"
        self.master = "http://172.30.67.7:5050"
        self.slaves= ["172.30.67.5:5051","172.30.67.4:5051"] #slaves[i] = slave(1) always! 
        if username and password:
            self._user = username
            self._password = password
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=level)
        self.logger = logging.getLogger(__name__)

    def _list_all(self):
        """List all jobs on Chronos."""
        return self._call( self.chronos+"/v1/scheduler/jobs", "GET")

    def _list(self,job_name):
        """List all jobs on Chronos."""
        return self._call(self.chronos+"/v1/scheduler/jobs/search?name="+job_name, "GET")

    def _list_jobs_graph(self):
        """ Send command /scheduler/graph/csv  and  returns  data  as 
             node,myjob1,fresh,running
             node,myjob2,failure,idle """
        return self._call(self.chronos+'/v1/scheduler/graph/csv', "GET")
  
    def _obtain_mesos_jobs(self):
        """Obtains the list of jobs in Mesos"""
        return self._call(self.master+'/master/tasks.json',"GET")

    def _obtain_mesos_nodes(self):
        """Obtains the list of nodes in Mesos"""
        return self._call(self.master +'/master/slaves',"GET")

    def _obtain_chronos_slaveX_state(self, slaveX_hostname):
       """get the state info of a slave slaveX_hostname is slave(1)@172.30.67.4:5051"""
       pos=slaveX_hostname.find("@")
       slaveX=slaveX_hostname[:pos]
       if slaveX_hostname.find(self.slaves[0])>=0:
         slave="http://"+self.slaves[0]
       else:
         slave="http://"+self.slaves[1]
       path = slave+"/"+slaveX+"/state"
       log.debug( "PATH_STATE is %s", path)
       return self._call(path,"GET")

    def get(self,name):
        """List name job on Chronos."""
        path = self.chronos+"/v1/scheduler/job/%s" % name
        return self._call(path, "GET")

    def delete(self, name):
        """Delete a job by name"""
        path = self.chronos+"/v1/scheduler/job/%s" % name
        return self._call(path, "DELETE")

    def delete_tasks(self, name):
        """Terminate all tasks for a running/stuck job"""
        path = self.chronos+"v1/scheduler/task/kill/%s" % name
        return self._call(path, "DELETE")

    def run(self, name):
        log.debug("Run a job by name")
        path = "/v1/scheduler/job/%s" % name
        return self._call(path, "PUT")

    def add(self, job_def, update=False):
        log.debug("Schedule a new job")
        path = self.chronos + "/v1/scheduler/iso8601"
        self._check_fields(job_def)
        if "parents" in job_def:
            path = self.chronos+"/v1/scheduler/dependency"
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
        return self._call('/v1/scheduler/job/stat/%s' % name, "GET")

    def scheduler_stat_99th(self):
        return self._call('/v1/scheduler/stats/99thPercentile', 'GET')

    def scheduler_stat_98th(self):
        return self._call('/v1/scheduler/stats/98thPercentile', 'GET')

    def scheduler_stat_95th(self):
        return self._call('/v1/scheduler/stats/95thPercentile', 'GET')

    def scheduler_stat_75th(self):
        return self._call('/v1/scheduler/stats/75thPercentile', 'GET')

    def scheduler_stat_median(self):
        return self._call('/v1/scheduler/stats/median', 'GET')


    def _call(self, url, method="GET", body=None, headers={}):
        hdrs = {}
        if body:
            hdrs['Content-Type'] = "application/json"
        hdrs.update(headers)
        log.debug("Fetch: %s %s" % (method, url))
        if body:
            log.debug("Body: %s" % body)
        conn = httplib2.Http(disable_ssl_certificate_validation=True)
        #log.debug("FIND: %s" % self.chronos)
        #log.debug("FIND: %d" % url.find(self.chronos))
        
        if (url.find(self.chronos) >= 0):
          if self._user and self._password:
            #log.debug("Credentials set!")
            conn.add_credentials(self._user, self._password)

        response = None
        #server=self.chronos
        endpoint = "%s" % (url)
        #endpoint = "%s%s" % (server, url)
        try:
                resp, content = conn.request(endpoint, method, body=body, headers=hdrs)
        except (socket.error, httplib2.ServerNotFoundError) as e:
                log.debug('Error while calling %s: %s. Retrying', endpoint, e.message)
                
        try:
                response = self._check(resp, content)
                return response
        except ChronosAPIError as e:
                log.debug('Error while calling %s: %s', endpoint, e.message)

  

    def _check(self, resp, content):
        status = resp.status
        log.debug("status: %d" % status)
        #self.logger.debug("status: %d" % status)
        payload = None

        if status == 401:
            raise UnauthorizedError('Not Authorized,look your credentials!')

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


class PyMesosJobRunner(AsynchronousJobRunner):
    """Job runner backed by a finite pool of worker threads. FIFO scheduling
    """
    runner_name = "PyMesosRunner"

    def __init__(self, app, nworkers, **kwargs):
        assert chronos is not None, PYMESOS_IMPORT_MESSAGE
        log.debug("Loading app %s", app)
        runner_param_specs = dict(chronos_server=dict(map=str),user=dict(map=str), password=dict(map=str))
            
        if 'runner_param_specs' not in kwargs:
            kwargs['runner_param_specs'] = dict()
        kwargs['runner_param_specs'].update(runner_param_specs)

        """Start the job runner parent object """
        super(PyMesosJobRunner, self).__init__(app, nworkers, **kwargs)
        
        self.chronos_cli = ChronosClient(self.runner_params["chronos_server"], username=self.runner_params["user"],
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
            log.debug("Job creation failure!! No Response from PyMesos")
            job_wrapper.fail("Not submitted")
        else:
            log.debug("ChronosTask created " + job_id)
            # Create an object of AsynchronousJobState and add it to the monitor queue.
            ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper, job_id=job_id, job_destination=job_destination)
            self.monitor_queue.put(ajs)
        return None 

    def connect(servers, username="admin", password=None):
        return ChronosClient(servers, username=username, password=password)

    def _produce_pymesos_job_name(self, job_id):
        # wrapper.get_id_tag() instead of job_id for compatibility with TaskWrappers.
        return "ChronosTask_" + str(job_id)

    def _obtain_chronos_job_sandbox_path(self, job_id):
        '''Method to obtain the slaves' hostnames that are executing chronos jobs'''
        slaveX_hostname=self._obtain_chronos_jobs_nodes(job_id)

        #chronos_job_sandbox_path=[]

        chronos_task_name = "ChronosTask:" + self._produce_pymesos_job_name(job_id)
        log.debug("chronos_task_name is %s", chronos_task_name)
        
        state_info=self.chronos_cli._obtain_chronos_slaveX_state(slaveX_hostname)
        if state_info:
            log.debug("SONO NELLO STATE")
            
            for framework  in state_info['completed_frameworks']:
              log.debug("LUNGHEZZA DEI COMPL EXEC %d",len(framework['completed_executors']))
              for executor in framework['completed_executors']:
                     task_item=executor['completed_tasks'][0]
                     
                     if (chronos_task_name == task_item['name']):
                        log.debug("SANDBOX PATH IS %s:", executor['directory'])
                        chronos_job_sandbox_path = executor['directory']
                        
        """/var/lib/mesos/slaves/"+ task['slave_id'] + "/frameworks/" + task['framework_id'] + \
        "/executors/"+ task['id']+"/runs/"+ task['container']"""
        
        return chronos_job_sandbox_path

    def _obtain_chronos_jobs_nodes(self, job_id):
        '''Method to obtain the slaves' hostnames that are executing chronos jobs'''
        mesos_jobs = self.chronos_cli._obtain_mesos_jobs()

        chronos_task_name = "ChronosTask:" + self._produce_pymesos_job_name(job_id)
        if mesos_jobs:
            for mesos_job in mesos_jobs['tasks']:
                if chronos_task_name == mesos_job['name']:
                    mesos_nodes = self.chronos_cli._obtain_mesos_nodes()
                    if mesos_nodes:
                        for mesos_node in mesos_nodes['slaves']:
                            if mesos_node['id'] == mesos_job['slave_id']:
                                #"pid":"slave(1)@172.30.67.4:5051"--->slave(1)
                                """pid=mesos_node['pid']
                                pos=pid.find("@")
                                chronos_job_slave=pid[:pos]"""
                                chronos_job_slave=mesos_node['pid']
                                log.debug("JOB SLAVE IS %s", chronos_job_slave)
        """ return slave(1)"""
        return chronos_job_slave

    def post_task(self, job_wrapper):
        """ Sumbit job to Mesos cluster and return jobid
            Create Job model schema of PyMesos and call the http_post_request method.
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
            if (job_destination.params["pymesos_volumes_containerPath"]):
             volume = job_destination.params["pymesos_volumes_containerPath"]
             volume = volume.split(",")
             log.debug("VOLUME is s%", volume)
             for i in volume:
                temp = dict({"containerPath":job_destination.params["pymesos_volumes_containerPath"],              
                             "hostPath":job_destination.params["pymesos_volumes_hostPath"],"mode":"RW"})
                volumes.append(temp)
        except:
                log.debug("pymesos_volumes not set. Getting default volume!!")

        try:
            #if (job_destination.params["pymesos_volumes_containerPath"]):
             #       src_command = job_wrapper.runner_command_line
             #       command = src_command.replace(volumes[0]["hostPath"],volumes[0]["containerPath"])
             #       log.debug("NEW COMMAND IS %s",command)
            #else:
                    
               command = job_wrapper.runner_command_line
        except:
               command = job_wrapper.runner_command_line
        
        
        pymesos_jobname=self._produce_pymesos_job_name(job_wrapper.job_id)
        pymesos_job = {
              "name": pymesos_jobname,
              "command": command,
              "schedule":"R1//P10M", 
              "scheduleTimeZone":"LMT",
              "epsilon": "PT60S",
              "owner": None,
              "shell":True,
              "async":False,
              "container": {
                "type": "DOCKER",
                "image":job_destination.params["pymesos_default_container_id"], # self._find_container(job_wrapper).container_id,
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
        result = self.chronos_cli.add(pymesos_job)
                
        log.debug("Response from pymesos task :  %s" % result)  
           
        return pymesos_jobname
    

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
                        log.debug("TROVATO %s con state %s", job_id,properties[3])
                        return properties[3]
    
    def create_log_file(self,job_state,returncode):
        """ Create log files in galaxy, namely error_file, output_file, exit_code_file
            Return true, if all the file creations are successful
        """
        
        
        job_destination= job_state.job_wrapper.job_destination
        job_id=job_state.job_wrapper.job_id
        job_name=self._produce_pymesos_job_name(job_id)
        if job_destination.params["docker_enabled"]:
           path_sandbox=self._obtain_chronos_job_sandbox_path(job_id)
           
           """under path_sandbox there are stderr e stdout"""
        log.debug("path_sandbox is %s",path_sandbox)
        if path_sandbox:
            slaveX_hostname=self._obtain_chronos_jobs_nodes(job_id)
            if slaveX_hostname.find(self.chronos_cli.slaves[0])>=0:
               slave="http://"+self.chronos_cli.slaves[0]
            else:
               slave="http://"+self.chronos_cli.slaves[1]

            path = slave + "/files/download.json?path=" + path_sandbox
            log.debug("PATH FOR DOWNLOAD is %s", path)
            """ content of the API callas"""
            chroj_output_file = self.chronos_cli._call(path + "/stdout","GET")
            chroj_error_file  = self.chronos_cli._call(path + "/stderr","GET")
 
            try:
                # Read from GoChronos output_file and write it into galaxy output_file.
                temp_file = open("temp_out_file", "w")
                temp_file.write(chroj_output_file)
                temp_file.close()
                
                out = open(job_state.output_file, "w")
                temp_file = open('temp_out_file', 'r')
                temp_lines=temp_file.readlines()
                num_lines=len(temp_lines)
                #log.debug("LINEE OUTPUT %d",num_lines)
                for i in range(0,num_lines):
                    if 'Starting task' in temp_lines[i]:
                        from_index=i
                        break

                for i in range(from_index+1,num_lines):
                        out.write(temp_lines[i])
                out.close()
                temp_file.close()
                
                # Read from GoChronos error_file and write it into galaxy error file
                
                log_file = open(job_state.error_file, "w")
                if (returncode != "0"):    
                  log_file.write(chroj_error_file)
                else:
                  log_file.write('')
                log_file.close()
                 
                # Read from GoMesos exit_code and write it into galaxy exit_code_file.
                #out_log = returncode  "0" OK  e "1" ERROR
                log_file = open(job_state.exit_code_file, "w")
                log_file.write(returncode)
                log_file.close()
                
                log.debug("CREATE OUTPUT FILE: " + str(job_state.output_file))
                log.debug("CREATE ERROR FILE: " + str(job_state.error_file))
                log.debug("CREATE EXIT CODE FILE: " + str(job_state.exit_code_file))
                return True
            except IOError as e:
                log.error('Could not access task log file %s' % str(e))
                log.debug("IO Error occurred when accessing the files!!")
                return False

        log.debug("NESSUN FILE SCARICATO!")
        return True

    def check_watched_item(self, job_state):
        log.debug("Checks the state of a job already submitted on GoChronos. Job state is a AsynchronousJobState\n")
        log.debug("job_state.job_wrapper.job_id %d" % job_state.job_wrapper.job_id)
        pymesos_job_name=self._produce_pymesos_job_name(job_state.job_wrapper.job_id)
        response=self.chronos_cli._list(pymesos_job_name)
         
        
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
                job_chronos_status="0"
                self.create_log_file(job_state, job_chronos_status)
                self.mark_as_finished(job_state)
                return None
            elif failed:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.ERROR)
                job_chronos_status="1"
                self.create_log_file(job_state, job_chronos_status)
                self.mark_as_failed(job_state)
                return None
            elif (self._get_chronos_job_state(pymesos_job_name)).find("running")>=0:
                job_state.running = True
                job_state.job_wrapper.change_state(model.Job.states.RUNNING)
                return job_state

            elif (self._get_chronos_job_state(pymesos_job_name)).find("queued")>=0:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.QUEUED)
                #log.debug("SONO IN CODA!")
                return job_state


        elif len(response) == 0:
            # there is no job responding to this job_id, it is either lost or something happened.
            self.create_log_file(job_state,"1")
            self.mark_as_failed(job_state)
            return job_state
        else:
            # there is more than one job associated to the expected unique job id used as selector.
            log.error("There is more than one Chronos Job associated to job id " + job_state.job_id)
            job_state.job_wrapper.change_state(model.Job.states.ERROR)
            self.create_log_file(job_state,"1")
            self.mark_as_failed(job_state)
            return job_state

        """ def fail_job(self, job_state):  NON E' DETTO CHE SERVA SOVRASCRIVERLA
         """
    def stop_job(self, job):
        """Attempts to delete a dispatched job to the mesos cluster"""
        try:
            job_name=self._produce_pymesos_job_name(job.id)
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
        

   
     

