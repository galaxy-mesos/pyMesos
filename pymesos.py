#!/home/galaxy/galaxy/.venv/bin/python
"""Offload jobs to a Mesos cluster.
"""


import logging
import socket
import httplib2
import json

import time
import os
import inspect
import requests
from datetime import datetime


from galaxy import model
from galaxy.jobs.runners import AsynchronousJobState, AsynchronousJobRunner
from os import environ as os_environ
from six import text_type
from requests import Request, Session
from galaxy.util import specs
from base64 import b64encode
from bioblend import galaxy


# Chronos package imports:
try:
    import chronos     

except ImportError as exc:
    chronos = None
    PYMESOS_IMPORT_MESSAGE = ('The Python Chronos Client package is required to use '
                          'this feature, please install it or correct the '
                          'following error:\nImportError %s' % str(exc))

DEFAULT_GALAXY_HOME = "/home/galaxy/galaxy"

MESOS_PARAM_SPECS = dict(
    password_chronos=dict(
        map=specs.to_str_or_none,
        default=None
    ),
    user_chronos=dict(
        map=specs.to_str_or_none,
        default=None,
    ),
    master_server=dict(
        map=specs.to_str_or_none,
        default=None,
    ),
    chronos_server=dict(
        map=specs.to_str_or_none,
        default=None,
    ),
    galaxy_url=dict(
        map=specs.to_str_or_none,
        default=None,
    ),
    galaxy_api_key=dict(
        map=specs.to_str_or_none,
        default="GALAXY_ADMIN_API_KEY",
    ),
    mesos_sandbox=dict(
        map=specs.to_str_or_none,
        default="/mnt/mesos/sandbox/",
    ),
)
class ChronosAPIError(Exception):
     pass

class UnauthorizedError(Exception):
     pass


log = logging.getLogger(__name__)

__all__ = ['PyMesosJobRunner']



class ChronosClient(object):
    _user = None
    _password = None

    def __init__(self,chronos_server,master_server, username=None, password=None, level='WARN'):
       
        self.chronos = chronos_server
        log.debug('CHRONOS SERVER is %s', chronos_server)
        #self.master = "http://172.30.67.7:5050"
        self.master = master_server
        log.debug('MASTER SERVER is %s', master_server)
        if username and password:
            self._user_chronos = username
            self._password_chronos = password
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
       slave=slaveX_hostname[pos+1:]
       log.debug("SLAVE IP IS %s",slave)
       slave="http://"+ slave
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

    def _call(self, url, method="GET", body=None, headers={}):
        hdrs = {}
        if body:
            hdrs['Content-Type'] = "application/json"
        hdrs.update(headers)
        log.debug("Fetch: %s %s" % (method, url))
        if body:
            log.debug("Body: %s" % body)
        conn = httplib2.Http(disable_ssl_certificate_validation=True)
        
        if (url.find(self.chronos) >= 0):
          if self._user_chronos and self._password_chronos:
            log.debug("Credentials set!")
            conn.add_credentials(self._user_chronos, self._password_chronos)

        response = None
       
        endpoint = "%s" % (url)
       
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
                log.debug("Response valid json: %s" % content)
            except ValueError:
                log.debug("Response not  json: %s" % content)
                payload = content

        if payload is None and status != 204:
            raise ChronosAPIError("Request to Chronos API failed: status: %d, response: %s" % (status, content))

        return payload


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

    def __init__(self, app, nworkers, **kwds):
        assert chronos is not None, PYMESOS_IMPORT_MESSAGE
        log.debug("Loading app %s", app)

        super( PyMesosJobRunner, self ).__init__( app, nworkers, runner_param_specs=MESOS_PARAM_SPECS, **kwds )
        
        galaxy_url = self.runner_params.galaxy_url
        self.mesos_sandbox_dir = self.runner_params.mesos_sandbox
        log.debug("MESOS SANDBOX %s",self.mesos_sandbox_dir)
        if not galaxy_url:
            galaxy_url = app.config.galaxy_infrastructure_url+"/"
        log.debug("GALAXY URL %s",galaxy_url)
        self.galaxy_url = galaxy_url
        self.galaxy_admin_key=self.runner_params.galaxy_api_key
        self.galaxy_inst = galaxy.GalaxyInstance(self.galaxy_url, key=self.galaxy_admin_key)

        self.chronos_cli = ChronosClient(self.runner_params.chronos_server,
                                         self.runner_params.master_server,
                                         username=self.runner_params.user_chronos,
                                         password=self.runner_params.password_chronos)
        
        
        if not self.chronos_cli:
            log.debug("Connection failure!! Runner cannot be started")
        else:
            
            self._init_monitor_thread()
            self._init_worker_threads()

    def add_chronos_dataset(self,struct,file_name,url):
        struct.append({'file_name': file_name, 'url': url}) 

    def list_galaxy_jobs(self):
        return self.galaxy_inst.jobs.get_jobs()

    def get_galaxy_dataset_info(self,id):
        return self.galaxy_inst.datasets.show_dataset(id)

    def get_galaxy_job_param_info(self,encoded_job_id,param_to_search):
        job_data=self.galaxy_inst.jobs.show_job(encoded_job_id,full_details=True)
        if job_data:
         if param_to_search in job_data['params'].iterkeys():
             return job_data['params'][param_to_search]
 

    def get_galaxy_reference_data_info(self,galaxy_command,genome_index):
        reference_data_toChronos=[]
        tool_data_table_info={}
        tool_data_table={}
        tool_data_list=self.galaxy_inst.tool_data.get_data_tables()
        for item in tool_data_list:
          log.debug("TOOL TABLE %s", item['name']) 
          tool_data_table_info=self.galaxy_inst.tool_data.show_data_table(item['name'])
          if tool_data_table_info.has_key('columns'):
           tool_data_table=dict((str(key), value) for (key, value) in tool_data_table_info.iteritems())
        
           if "path" in tool_data_table['columns']  and len(tool_data_table['fields'])>0:
              base_url_path=self.galaxy_url+'api/tool_data/'+ item['name']+'/fields/'+ genome_index
              url_path=base_url_path+"?key="+self.galaxy_admin_key
              print url_path
              ref_info_genome=self.chronos_cli._call(url_path,"GET")
              print ref_info_genome
              path_to_check=ref_info_genome['base_dir'][0]
              print path_to_check
              if galaxy_command.find(path_to_check)>=0:

               for key in ref_info_genome['files']:
                 #it means that right tool data table  found 
                 #file_name=path_to_check wo 1st / +"/"+key
                 file_name=path_to_check[1:]+"/"+key
                 url=base_url_path+'/files/'+key+'?key='+self.galaxy_admin_key
                 log.debug("REFERENCE FILE %s  URL%s",file_name,url)
                 self.add_chronos_dataset(reference_data_toChronos,file_name,url)
              break
       
        return reference_data_toChronos
   
           
    def get_galaxy_job_input_info (self,encoded_job_id):

       job_data=self.galaxy_inst.jobs.show_job(encoded_job_id,full_details=True)
       input_dataset_ids=[]
       chronos_inputs_dataset=[]
       if job_data:
            for key in job_data['inputs'].iterkeys():
                input_dataset_ids.append(job_data['inputs'][key]['id'])
                log.debug('INPUT ID %s',job_data['inputs'][key]['id'])
       if input_dataset_ids:
          for id in input_dataset_ids:
               dataset_input_info=self.get_galaxy_dataset_info(id)
               pos=dataset_input_info["file_name"].find("dataset_")
               file_name=dataset_input_info["file_name"][pos:]
               log.debug("FILENAME INPUT %s", file_name)
               url=dataset_input_info["download_url"]
               self.add_chronos_dataset(chronos_inputs_dataset,file_name,url)

       return chronos_inputs_dataset

    def get_galaxy_job_output_info (self,encoded_job_id):

      job_data=self.galaxy_inst.jobs.show_job(encoded_job_id,full_details=True)
      output_dataset_ids=[]
      chronos_outputs_dataset=[]
      if job_data:
            for key in job_data['outputs'].iterkeys():
               output_dataset_ids.append(job_data['outputs'][key]['id'])
      if output_dataset_ids:         
         for id in output_dataset_ids:
               dataset_output_info=self.get_galaxy_dataset_info(id)
               pos=dataset_output_info["file_name"].find("dataset_")
               file_name=dataset_output_info["file_name"][pos:]
               self.add_chronos_dataset(chronos_outputs_dataset,file_name,url="")

      return chronos_outputs_dataset

      
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

        chronos_task_name = "ChronosTask:" + self._produce_pymesos_job_name(job_id)
        #log.debug("chronos_task_name is %s", chronos_task_name)
        chronos_job_sandbox_path="No Path"
        state_info=self.chronos_cli._obtain_chronos_slaveX_state(slaveX_hostname)
        if state_info:
            
            for framework  in state_info['completed_frameworks']:
              #log.debug("LUNGHEZZA DEI COMPLETED EXECUTORS %d",len(framework['completed_executors']))
              for executor in framework['completed_executors']:
                     for task_item in executor['completed_tasks']:
                     
                       if (chronos_task_name == task_item['name']):
                           #log.debug("SANDBOX PATH IS %s:", executor['directory'])
                           chronos_job_sandbox_path = executor['directory']
                        
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
                               
                                chronos_job_slave=mesos_node['pid']
                                log.debug("JOB SLAVE IS %s", chronos_job_slave)

        return chronos_job_slave

    def get_galaxy_encoded_job_id(self, base_command):
        '''xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'''
        galaxy_jobs = self.list_galaxy_jobs()
        if galaxy_jobs:
       
            for job  in galaxy_jobs:
                
                if job["command_line"] == base_command:
                    job_api_id=job["id"]
                    log.debug("ENCODED ID E COMMAND %s %s", job_api_id,base_command)
                    return job_api_id

    def get_galaxy_command_for_docker(self,galaxy_command,dataset_galaxy_dir,env_variables=[]):
       
       command=galaxy_command
       for i in env_variables:
          if i["name"]=="REF_INDEX_PATH":
           #replace galaxy ref data dir with absolute container ref data dir
           command=command.replace("/"+i["value"],self.mesos_sandbox_dir+i["value"])
           break
       command=command.replace(dataset_galaxy_dir,self.mesos_sandbox_dir)
       log.debug("COMMAND FOR DOCKER %s",command)
       return command



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
        
        workingDirectory=job_wrapper.working_directory
        job_tool=job_wrapper.tool
        if self._find_container(job_wrapper):
              log.debug("WRAPPER CONTAINER IMAGE %s",self._find_container(job_wrapper).container_id)
              docker_image = self._find_container(job_wrapper).container_id
        else:
              log.debug("DOCKER IMAGE: %s \n",job_destination.params["pymesos_default_container_id"])
              docker_image = job_destination.params["pymesos_default_container_id"]
        log.debug("DOCKER IMAGE: %s \n",docker_image)
        log.debug("Job tool: %s",job_tool)
        
        volumes = []
        try:
         if (job_destination.params["pymesos_volumes"]):
             volume = job_destination.params["pymesos_volumes"]
             volume = volume.split(",")
             log.debug("VOLUME is s%", volume)
             for i in volume:
                temp = dict({"containerPath":i,              
                             "hostPath":i,"mode":"RW"})
                volumes.append(temp)
        except:
          log.debug("NO VOLUMES")

        #prepare galaxy command for docker and fetch data
        base_command=job_wrapper.get_command_line()
        log.debug("BASE_COMMAND_LINE: %s",base_command)
        encoded_job_id=self.get_galaxy_encoded_job_id(job_wrapper.get_command_line())
        log.debug("ENCODED_JOB_ID: %s",encoded_job_id)
        dataset_dir= DEFAULT_GALAXY_HOME + job_destination.params["file_path"]
        log.debug("DATASET_DIR: %s",dataset_dir)
        
        pymesos_jobname=self._produce_pymesos_job_name(job_wrapper.job_id)

        fetch_data = []
        inputs=[]
        env_vars=[]
        genome_data={}
        cache=False
        extract=False
        executable=False
        
        log.debug("PREPARE FETCH DATA!!")
        inputs=self.get_galaxy_job_input_info(encoded_job_id)
        galaxy_server=self.galaxy_url.replace("/galaxy/","")
        for i in inputs:
            fetch_data.append({"uri":galaxy_server+i["url"],"cache":cache,"extract":extract,"executable":executable,"output_file":i["file_name"]})
        #just only if genomesource=indexed right tool data table indexes must be fetched
        try:
         genome_data=json.loads(self.get_galaxy_job_param_info(encoded_job_id,"refGenomeSource"))
         if genome_data.has_key('genomeSource'):
           if genome_data['genomeSource'] == "indexed":
              genome_index=genome_data['index']
              reference_data_files=self.get_galaxy_reference_data_info(job_wrapper.get_command_line(),genome_index)
              for f in reference_data_files:
                fetch_data.append({"uri":f["url"],"cache":cache,"extract":extract,"executable":executable,"output_file":f["file_name"]})
              if len(reference_data_files)>0:
               ref_path=fetch_data[len(fetch_data)-1]["output_file"]
               last_slash=ref_path.rfind("/")
               ref_path=ref_path[:last_slash]
               log.debug("REF PATH %s",ref_path)
               env_vars.append({"name": "REF_INDEX_PATH", "value":ref_path})
        
        except:
             log.debug("NO REFERENCE FETCH DATA")

        docker_command=self.get_galaxy_command_for_docker(base_command,dataset_dir,env_vars)

        pymesos_job = {
              "name": pymesos_jobname,
              "command": docker_command,
              "schedule":"R1//P10M", 
              "scheduleTimeZone":"LMT",
              "epsilon": "PT60S",
              "owner": None,
              "shell":True,
              "async":False,
              "container": {
                "type": "DOCKER",
                "image": docker_image,
                "volumes": volumes,
                "forcePullImage":True
              },
              "successCount": 0,
              "errorCount": 0, 
              "cpus": mesos_task_cpu,
              "mem": mesos_task_mem,
              "disk":mesos_task_disk,
              "fetch": fetch_data,  
              "dataJob": False,
              "environmentVariables":env_vars,
              "constraints":[]
            }
         
        log.debug("\n JOB POST TASK TO BE EXECUTED \n")
        result = self.chronos_cli.add(pymesos_job)
                
        log.debug("Response from pymesos task :  %s" % result)  
           
        return pymesos_jobname
    

    def _get_chronos_job_state(self, job_id):
        '''Given a job id, calls Chronos to know the state of that job'''
        chronos_jobs = self.chronos_cli._list_jobs_graph()
        myjob_state=""
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
        encoded_job_id=self.get_galaxy_encoded_job_id(job_state.job_wrapper.get_command_line())
        log.debug("CREATE LOG FILE, ENCODED JOB ID %s",encoded_job_id)
        dataset_dir= DEFAULT_GALAXY_HOME + job_destination.params["file_path"]
        job_name=self._produce_pymesos_job_name(job_id)
        if job_destination.params["docker_enabled"]:
           path_sandbox=self._obtain_chronos_job_sandbox_path(job_id)
           
           """under path_sandbox there are stderr e stdout and also output files for galaxy"""
        if path_sandbox:
            slaveX_hostname=self._obtain_chronos_jobs_nodes(job_id)
            pos=slaveX_hostname.find("@")
            slave=slaveX_hostname[pos+1:]
            log.debug("SLAVE IP IS %s",slave)
            slave="http://"+ slave
            path = slave + "/files/download?path=" + path_sandbox
            log.debug("PATH FOR DOWNLOAD is %s", path)

            output_datasets_name=[]
            output_datasets_name=self.get_galaxy_job_output_info (encoded_job_id)
            
            """ content of the API calls"""
            chroj_error_file  = self.chronos_cli._call(path + "/stderr","GET")
            chroj_stdout_file = self.chronos_cli._call(path + "/stdout","GET")
            
            for name in output_datasets_name:
               gxy_output_file=""
               gxy_output_file = self.chronos_cli._call(path + "/" + name["file_name"],"GET")
               
               if gxy_output_file is not None:
                  dataset_name_path=dataset_dir + name["file_name"]
                  log.debug("DATASET OUTPUT FILE %s",dataset_name_path)
                  output_file = open(dataset_name_path, "w")
                  log.debug("DATASET OUTPUT FILE CREATO!")
                  output_file.write(gxy_output_file)
                  output_file.close()
                  

            logs_file = open(job_state.output_file, "w")
            #logs_file.write(str(chroj_stdout_file)) really is more usefull stderr output
            logs_file.write(str(chroj_error_file))
            log.debug("JOB STATE OUTPUT FILE CREATO")
            logs_file.close()
                
            log_file = open(job_state.error_file, "w")
            if (returncode != "ok"):
                log_file.write(str(chroj_error_file))
                log.debug("JOB STATE ERROR FILE CREATO")
            else:
                log_file.write('')
            log_file.close()
    

        else:
        #surely something was wrong if no container path sandbox
            with open(job_state.error_file, 'w') as fil:
                fil.write("Chronos Job %s",job_name,"= galaxy job %s",job_id,"has failed")
            returncode="error"
            
        #out_log = returncode  "0" OK  e "1" ERROR
        log_file = open(job_state.exit_code_file, "w")
        log_file.write(returncode)
        log_file.close()
                
        log.debug("CREATE OUTPUT FILE: " + str(job_state.output_file))
        log.debug("CREATE ERROR FILE: " + str(job_state.error_file))
        log.debug("CREATE EXIT CODE FILE: " + str(job_state.exit_code_file))
        return True
           


    def check_watched_item(self, job_state):
        log.debug("Checks the state of a job already submitted on Chronos. Job state is a AsynchronousJobState\n")
        log.debug("job_state.job_wrapper.job_id %d" % job_state.job_wrapper.job_id)
        pymesos_job_name=self._produce_pymesos_job_name(job_state.job_wrapper.job_id)
        response=self.chronos_cli._list(pymesos_job_name)
        succeeded=0
        failed=0
        if len(response)==1:
            if response[0]['successCount']>=1: 
                    succeeded = response[0]['successCount']
            if response[0]['errorCount']>=1: 
                    failed = response[0]['errorCount']
                    
            if succeeded:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.OK)
                #job_chronos_status="0"
                self.create_log_file(job_state, model.Job.states.OK)
                self.mark_as_finished(job_state)
                return None
            elif not succeeded and not failed:
                job_state.running = True
                job_state.job_wrapper.change_state(model.Job.states.RUNNING)
                return job_state
            elif failed:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.ERROR)
                #job_chronos_status="1"
                self.create_log_file(job_state,model.Job.states.ERROR)
                self.mark_as_failed(job_state)
                return None

        elif len(response) == 0:
            log.error("There is no response Chronos Job associated to job id " + job_state.job_id)
            # there is no job responding to this job_id, it is either lost or something happened.
            self.create_log_file(job_state,model.Job.states.ERROR)
            self.mark_as_failed(job_state)
            return job_state
        else:
            # there is more than one job associated to the expected unique job id used as selector.
            log.error("There is more than one Chronos Job associated to job id " + job_state.job_id)
            job_state.job_wrapper.change_state(model.Job.states.ERROR)
            self.create_log_file(job_state,model.Job.states.ERROR)
            self.mark_as_failed(job_state)
            return job_state

    def fail_job( self, job_state):
        """Seperated out so we can use the worker threads for it."""
        with open(job_state.error_file, 'r') as outfile:
            stdout_content = outfile.read()

        if getattr(job_state, 'stop_job', True):
            self.stop_job(self.sa_session.query(self.app.model.Job).get(job_state.job_wrapper.job_id))
        self._handle_runner_state('failure', job_state)
        # Not convinced this is the best way to indicate this state, but
        # something necessary
        if not job_state.runner_state_handled:
            job_state.job_wrapper.fail(
                message=getattr(job_state, 'fail_message', 'Job failed'),
                stdout="See stderr for job failure", stderr=stdout_content
            )
        
       


    def stop_job(self, galaxy_job_id):
        """Attempts to delete a dispatched job to the mesos cluster"""
        try:
            job_name=self._produce_pymesos_job_name(galaxy_job_id)
            log.debug("STOP JOB EXECUTION OF JOB ID: " + job_name)
            self.chronos_cli.delete(job_name) 
            log.debug("Terminated at user's request %s" ,job_name)
        except Exception as e:
            log.debug("(%s) User killed running job, but error encountered during termination NO JOB STOPPED: %s" % (
                job_name,e))

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
        

   
     


