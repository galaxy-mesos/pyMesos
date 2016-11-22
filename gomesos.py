"""
Offload jobs to a Mesos cluster.
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


# Chronos package imports:
try:
    import chronos     

except ImportError as exc:
    chronos = None
    GOMESOS_IMPORT_MESSAGE = ('The Python Chronos Client package is required to use '
                          'this feature, please install it or correct the '
                          'following error:\nImportError %s' % str(exc))

log = logging.getLogger(__name__)

__all__ = ['GoMesosJobRunner']


class GoMesosJobRunner(AsynchronousJobRunner):
    """
    Job runner backed by a finite pool of worker threads. FIFO scheduling
    """
    runner_name = "GoMesosRunner"

    def __init__(self, app, nworkers, **kwargs):
        assert chronos is not None, GOMESOS_IMPORT_MESSAGE
        log.debug("Loading app %s", app)
        runner_param_specs = dict(master=dict(map=str),user=dict(map=str), password=dict(map=str))
            
        if 'runner_param_specs' not in kwargs:
            kwargs['runner_param_specs'] = dict()
        kwargs['runner_param_specs'].update(runner_param_specs)

        """Start the job runner parent object """
        super(GoMesosJobRunner, self).__init__(app, nworkers, **kwargs)
        
        """Creo l'oggetto Chronos client"""
        g_chronos_cli = ChronosClient(self.runner_params["master"], self.runner_params["user"], self.runner_params["password"])

        chronos_cli = g_chronos_cli.connect(self.servers,self._user,self._password)
        
        if not chronos_cli:
            log.debug("Connection failure!! Runner cannot be started")
        else:
            """ Following methods starts threads.
                These methods invoke threading.Thread(name,target)
                      which inturn invokes methods monitor() and run_next().
            """
            self._init_monitor_thread()
            self._init_worker_threads()
       

    def queue_job(self, job_wrapper):
        """Create Chronos job and submit it to Mesos cluster"""
        # prepare the job
        # We currently don't need to include_metadata or include_work_dir_outputs, as working directory is the same
        # were galaxy will expect results.
        log.debug("Starting queue_job for job " + job_wrapper.get_id_tag())
        if not self.prepare_job(job_wrapper, include_metadata=False, modify_command_for_container=False):
            return

        job_destination = job_wrapper.job_destination
        job_id = self.post_task(job_wrapper)
        if not job_id:
            log.debug("Job creation faliure!! No Response from GoMesos")
            job_wrapper.fail("Not submitted")
        else:
            #log.debug("Starting queue_job for job " + job_id)
            # Create an object of AsynchronousJobState and add it to the monitor queue.
            ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper, job_id=job_id, job_destination=job_destination)
            self.monitor_queue.put(ajs)
        return None ####

     def __produce_gomesos_job_name(self, job_wrapper):
        # wrapper.get_id_tag() instead of job_id for compatibility with TaskWrappers.
        return "galaxy-mesos" + job_wrapper.get_id_tag()

     def post_task(self, job_wrapper):
        """ Sumbit job to Mesos cluster and return jobid
            Create Job model schema of GoMesos and call the http_post_request method.
        """
        # Get the params from <destination> tag in job_conf by using job_destination.params[param]
        
            job_destination = job_wrapper.job_destination
            try:
                mesos_task_cpu = int(job_destination.params["mesos_task_cpu"])
            except:
                mesos_task_cpu = 1
            try:
                mesos_task_mem = int(job_destination.params["mesos_task_mem"])
            except:
                mesos_task_mem = 1
            try:
                docker_image = self._find_container(job_wrapper).container_id
                log.debug("DOCKER IMAGE: \n")
                log.debug(docker_image)
            except:
                log.debug("Error: Docker_image not specified in Job config and Tool config!!")
                return False
            ##  questi parametri avnno messi, project, volumes, etc...????? 

            try:
                log.debug(self.runner_params["gomesos_docker_project"])
                project = str(self.runner_params["gomesos_docker_project"])
            except KeyError:
                log.debug("gomesosdocker_project not defined, using defaults")
            try:
                log.debug(job_destination.params["gomesos_docker_volumes"])
                volume = job_destination.params["gomesos_docker_volumes"]
                volume = volume.split(",")
                for i in volume:
                    temp = dict({"name": i})
                    volumes.append(temp)
            except:
                log.debug("gomesos_volumes not set. Getting default volume!!")

            
            # Enable galaxy venv in the docker containers???  = false in conf.xml
            try:
                if(job_destination.params["virtualenv"] == "true"):
                    log.debug("Virtual environment is set!!!")
                    GALAXY_VENV_TEMPLATE = """GALAXY_VIRTUAL_ENV="%s"; if [ "$GALAXY_VIRTUAL_ENV" != "None" -a -z "$VIRTUAL_ENV" -a -f "$GALAXY_VIRTUAL_ENV/bin/activate" ]; then . "$GALAXY_VIRTUAL_ENV/bin/activate"; fi;"""
                    venv = GALAXY_VENV_TEMPLATE % job_wrapper.galaxy_virtual_env
                    command = "#!/bin/sh -c\n" + job_wrapper.working_directory + "\n" + venv + "\n" + job_wrapper.runner_command_line
                else:
                    command = "#!/bin/sh -c\n" + job_wrapper.working_directory + "\n" + job_wrapper.runner_command_line
            except:
                command = "#!/bin/sh -c\n" + job_wrapper.working_directory + "\n" + job_wrapper.runner_command_line

            # Construction of the Chronos Job object follows: https://mesos.github.io/chronos/docs/api.html#listing-jobs
            gomesos_jobname=self.__produce_gomesos_job_name(job_wrapper.get_id_tag())
            gomesos_job_json_hash = {
              "name": gomesos_jobname,
              "command": command,
              "schedule":R1//PT10M # inizia subito e ogni 10 minuti
               #self.__get_gomesos_job_schedule(output "Rn to repeat n times or R to repeat forever/YYYY-MM-DDThh:mm:ss.sTZD/
                                                          #"P(T is for distinguishing M(inute) and M(onth))),

              "scheduleTimeZone":LMT,
              "epsilon": None,
              "owner": None,
              "shell":true,
              "executor":"", #empty for non-async, default per il resto
              "executorFlags":"",
              "retries": 2,
              "async":true, #in galaxy sono tutti asincroni???
              #"parents": None, #if schedule=something,altrimenti schedule none e parents=parents, 
                                #al momento non so come distinguere da galaxy job scheduled o dipendenti da altri, per default "scheduled"
              "container": {
               "type": "DOCKER",
               "image": self._find_container(job_wrapper).container_id,
               "forcePullImage": false,
               "network": None,
               "volumes": []
                  #{
                   # "containerPath": "/var/log/",
                   # "hostPath": "/logs/",
                   # "mode": "RW"
                  #}
                #]
              },
              "successCount": 0,
              "errorCount": 0,
              "lastSuccess": "",
              "lastError": "", 
              "cpus": mesos_task_cpu,
              "mem": mesos_task_mem,
              "disk":mesos_task_disk,
              "uris": [],
              "runAsUser":None,
              "dataJob": false,
              "environmentVariables":[],
              "constraints":[]
            }

        # comando dirichiesta! bash curl -L -H 'Content-Type: application/json' -X POST -d '{json hash}' chronos-node:8080/scheduler/iso8601
         
        log.debug("\n JOB POST TASK TO BE EXECUTED \n")
            result = self.chronos_cli._add(gomesos_job_json_hash)
                
            
        log.debug("Response from gomesos task : " gomesos_jobname "is" + str(result.json()['content']) + "status code: " 
                        + str(result.json()['status']))

          
            # Return Status code 
            return gomesos_jobname
    
   #####se si vogliono aggiungere utility di Chronos in client.py####
    def list(self,job_name):
        """List all jobs on Chronos."""
        return self._call("/scheduler/job/search?name="+job_name, "GET")
    
    def __produce_gomesos_job_name(self, job_wrapper):
        # wrapper.get_id_tag() instead of job_id for compatibility with TaskWrappers.
        return "galaxy-mesos" + job_wrapper.get_id_tag() 

    def _list_jobs_graph(self):
        """ Send command /scheduler/graph/csv  and  returns  data  as 
             node,myjob1,fresh,running
             node,myjob2,failure,idle """
        return self._call('/scheduler/graph/csv', "GET")

    ''' get  possible job states
    CHRONOS job states: idle,running,queued,failed,started,finished,disabled,skipped,expired,removed
    (TODO: check if there are more possible CHRONOS job states)
    '''
    def _get_chronos_job_state(self, job_id):
        '''Given a job id, calls Chronos to know the state of that job'''
        chronos_jobs = self._list_jobs_graph()
        if chronos_jobs:
            parsed_chronos_jobs = chronos_jobs.split("\n")
            for chronos_job in parsed_chronos_jobs:
                if chronos_job != '':
                    properties = chronos_job.split(",")
                    # properties[1] --> Job name
                    if job_id == properties[1]:
                        # properties[3] --> Job state
                        return properties[3]
  ##### "container": {
    #"type": "DOCKER",
    #"image": "libmesos/ubuntu",
    #"network": "BRIDGE",
    #"volumes": [
     # {
      #  "name":"go-mesos",
      #  "containerPath": "/var/log/",
      #  "hostPath": "/logs/",
      #  "mode": "RW"
      #}]},
    
    def create_log_file(self, job_state, job_chronos):
        """ Create log files in galaxy, namely error_file, output_file, exit_code_file
            Return true, if all the file creations are successful
        """
        path = None
        for vol in job_chronos['container']['volumes']:
            if vol['name'] == "go-mesos":  ####?????? DA VEDERE DOPO
                path = str(vol['containerPath'])
        if path:
            chroj_output_file = path + "/chroj.log"
            chroj_error_file = path + "/chroj.err"
            try:
                # Read from GoMesos output_file and write it into galaxy output_file.
                f = open(chroj_output_file, "r")
                out_log = f.read()
                log_file = open(job_state.output_file, "w")
                log_file.write(out_log)
                log_file.close()
                f.close()
                # Read from GoMesos error_file and write it into galaxy error_file.
                f = open(chroj_error_file, "r")
                out_log = f.read()
                log_file = open(job_state.error_file, "w")
                log_file.write(out_log)
                log_file.close()
                f.close()
                # Read from GoMesos exit_code and write it into galaxy exit_code_file.
                out_log = str(job_chronos['result_fields']['status'])  #204/500/etc..DA VEDERE COME PRENDERLI!
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

#/usr/bin/curl -k -u admin:m3sosGalaxyC.2016 -L -H 'Content-Type: application/json' -X GET https://172.30.67.7:4443/scheduler/graph/csv
#node,SAMPLE_JOB1,success,idle
#node,test,success,idle
#https://github.com/mesos/chronos/issues/631

  ###############################################################################################
    def check_watched_item(self, job_state):
        """Checks the state of a job already submitted on Gomesos. Job state is a AsynchronousJobState"""
        gomesos_job_name=self.__produce_gomesos_job_name(job_state.job_wrapper.get_id_tag())
        json_job_data_resp=self.list(gomesos_job_name)
         
        response=json_job_data_resp.json()
        job_chronos=self.chronos_cli.create(json_job_data_resp) ####posso farlo???
        succeded=0
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
                self.create_log_file(job_state, job_chronos):
                self.mark_as_finished(job_state)
                return None
            elif failed:
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.ERROR)
                self.create_log_file(job_state, job_chronos)
                self.mark_as_failed(job_state)
                return None
            elif self._get_chronos_job_state(gomesos_job_name) == "running":
                job_state.running = True
                job_state.job_wrapper.change_state(model.Job.states.RUNNING)
                return job_state

            elif self._get_chronos_job_state(gomesos_job_name) == "queued":
                job_state.running = False
                job_state.job_wrapper.change_state(model.Job.states.QUEUED)
                return job_state


        elif len(response) == 0:
            # there is no job responding to this job_id, it is either lost or something happened.
            self.create_log_file(job_state, job_chronos)
            self.mark_as_failed(job_state)
            return job_state
        else:
            # there is more than one job associated to the expected unique job id used as selector.
            log.error("There is more than one Kubernetes Job associated to job id " + job_state.job_id)
            job_state.job_wrapper.change_state(model.Job.states.ERROR)
            self.create_log_file(job_state, job_chronos)
            self.mark_as_failed(job_state)
            return job_state

        """ def fail_job(self, job_state):  NON E' DETTO CHE SERVA SOVRASCRIVERLA
         """
    def stop_job(self, job):
        """Attempts to delete a dispatched job to the mesos cluster"""
        try:
            self.delete(self.__produce_gomesos_job_name(job.get_id_tag()))
            """if len(jobs.response['items']) >= 0:
                job_to_delete = Job(self._pykube_api, jobs.response['items'][0])
                job_to_delete.scale(replicas=0)"""
            # TODO assert whether job parallelism == 0
            # assert not job_to_delete.exists(), "Could not delete job,"+job.job_runner_external_id+" it still exists"
            log.debug("(%s/%s) Terminated at user's request" % (job.id, job.job_runner_external_id))
        except Exception as e:
            log.debug("(%s/%s) User killed running job, but error encountered during termination: %s" % (
                job.id, job.job_runner_external_id, e))

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
        

    def queue_job(self, job_wrapper):
        """Create Chronos job and submit it to Mesos cluster"""
        # prepare the job
        # We currently don't need to include_metadata or include_work_dir_outputs, as working directory is the same
        # were galaxy will expect results.
        log.debug("Starting queue_job for job " + job_wrapper.get_id_tag())
        if not self.prepare_job(job_wrapper, include_metadata=False, modify_command_for_container=False):
            return

        job_destination = job_wrapper.job_destination
        job_id = self.post_task(job_wrapper)   #----> crea il chronos job e lo invia a chronos
        if not job_id:
            log.debug("Job creation faliure!! No Response from GoMesos")
            job_wrapper.fail("Not submitted")
        else:
            #log.debug("Starting queue_job for job " + job_id)
            # Create an object of AsynchronousJobState and add it to the monitor queue.
            ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper, job_id=job_id, job_destination=job_destination)
            self.monitor_queue.put(ajs)
        return None ####

     
