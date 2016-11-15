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


# Chronos imports:
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
        
        log.debug("Loading app %s", app)
        runner_param_specs = dict(
            gomesos_master=dict(map=str),gomesos_user=dict(map=str), gomesos_password=dict(map=str))
            
        if 'runner_param_specs' not in kwargs:
            kwargs['runner_param_specs'] = dict()
        kwargs['runner_param_specs'].update(runner_param_specs)

        """Start the job runner parent object """
        super(GoMesosJobRunner, self).__init__(app, nworkers, **kwargs)
        
        """Creo l'oggetto Chronos client"""
        chronos_cli = ChronosClient(self.runner_params["gomesos_master"],proto="https", self.runner_params["gomesos_user"], 
                         self.runner_params["gomesos_password"],"WARN")
        self.chronos_cli = self.connect(self.runner_params["gomesos_master"],proto="https", self.runner_params["gomesos_user"], 
                         self.runner_params["gomesos_password"])
        
        if not self.chronos_cli:
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
  
        job_id = self.post_task(job_wrapper)   ----> crea il chronos job e lo invia a chronos
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
            ##  anche mesos_task_disk????? 

            try:
                log.debug(self.runner_params["godocker_project"])
                project = str(self.runner_params["godocker_project"])
            except KeyError:
                log.debug("godocker_project not defined, using defaults")
            try:
                log.debug(job_destination.params["godocker_volumes"])
                volume = job_destination.params["godocker_volumes"]
                volume = volume.split(",")
                for i in volume:
                    temp = dict({"name": i})
                    volumes.append(temp)
            except:
                log.debug("godocker_volumes not set. Getting default volume!!")

            
            # Enable galaxy venv in the docker containers  va fatta???? penso di si
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

           
            ###############################################àà

            # Construction of the Chronos Job object follows: https://mesos.github.io/chronos/docs/api.html#listing-jobs
            gomesos_jobname=self.__produce_gomesos_job_name(job_wrapper.get_id_tag())
            gomesos_job_json_hash = {
              "name": gomesos_jobname,
              "command": command,
              "schedule":R1//PT10M # inizia subito e ogni 10 minuti
               #self.__get_gomesos_job_schedule(output "Rn to repeat n times or R to repeat forever/YYYY-MM-DDThh:mm:ss.sTZD/
                                                          "P(T is for distinguishing M(inute) and M(onth))),

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
              "successCount": 0,   #cassandra presence
              "errorCount": 0,     # """
              "lastSuccess": "",     #ci vuole cassandra attaccata a chronos....non credo ci sia
              "lastError": "",    #  "
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
                
            )
            log.debug("Response from gomesos task : " gomesos_jobname "is" + str(result.json()['content']) + "status code: " 
                        + str(result.json()['status']))

        ###if status code ==OK ed e' asincrono allora avvisa chronos da mettere pero' nella funzione check_watched_item
          
            # Return Status code 
            return gomesos_jobname
    
   #####se si vogliono aggiungere utility di Chronos####
    def list(self,job_name):
        """List all jobs on Chronos."""
        return self._call("/scheduler/job/search?name="+job_name, "GET")

    def __get_k8s_job_spec_template(self, job_wrapper):
        """The k8s spec template is nothing but a Pod spec, except that it is nested and does not have an apiversion
        nor kind. In addition to required fields for a Pod, a pod template in a job must specify appropriate labels
        (see pod selector) and an appropriate restart policy."""
        k8s_spec_template = {
            "metadata": {
                "labels": {"app": self.__produce_unique_k8s_job_name(job_wrapper.get_id_tag())}
            },
            "spec": {
                "volumes": self.__get_k8s_mountable_volumes(job_wrapper),
                "restartPolicy": self.__get_k8s_restart_policy(job_wrapper),
                "containers": self.__get_k8s_containers(job_wrapper)
            }
        }
        # TODO include other relevant elements that people might want to use from
        # TODO http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_podspec

        return k8s_spec_template


    def __get_k8s_mountable_volumes(self, job_wrapper):
        """Provides the required volumes that the containers in the pod should be able to mount. This should be using
        the new persistent volumes and persistent volumes claim objects. This requires that both a PersistentVolume and
        a PersistentVolumeClaim are created before starting galaxy (starting a k8s job).
        """
        # TODO on this initial version we only support a single volume to be mounted.
        k8s_mountable_volume = {
            "name": self._galaxy_vol_name,
            "persistentVolumeClaim": {
                "claimName": self.runner_params['k8s_persistent_volume_claim_name']
            }
        }
        return [k8s_mountable_volume]

    def __get_k8s_containers(self, job_wrapper):
        """Fills in all required for setting up the docker containers to be used."""
        k8s_container = {
            "name": self.__get_k8s_container_name(job_wrapper),
            "image": self._find_container(job_wrapper).container_id,
            # this form of command overrides the entrypoint and allows multi command
            # command line execution, separated by ;, which is what Galaxy does
            # to assemble the command.
            # TODO possibly shell needs to be set by job_wrapper
            "command": ["/bin/bash", "-c", job_wrapper.runner_command_line],
            "workingDir": job_wrapper.working_directory,
            "volumeMounts": [{
                "mountPath": self.runner_params['k8s_persistent_volume_claim_mount_path'],
                "name": self._galaxy_vol_name
            }]
        }

        # if self.__requires_ports(job_wrapper):
        #    k8s_container['ports'] = self.__get_k8s_containers_ports(job_wrapper)

        return [k8s_container]

    # def __get_k8s_containers_ports(self, job_wrapper):

    #    for k,v self.runner_params:
    #        if k.startswith("container_port_"):

    def __assemble_k8s_container_image_name(self, job_wrapper):
        """Assembles the container image name as repo/owner/image:tag, where repo, owner and tag are optional"""
        job_destination = job_wrapper.job_destination

        # Determine the job's Kubernetes destination (context, namespace) and options from the job destination
        # definition
        repo = ""
        owner = ""
        if 'repo' in job_destination.params:
            repo = job_destination.params['repo'] + "/"
        if 'owner' in job_destination.params:
            owner = job_destination.params['owner'] + "/"

        k8s_cont_image = repo + owner + job_destination.params['image']

        if 'tag' in job_destination.params:
            k8s_cont_image += ":" + job_destination.params['tag']

        return k8s_cont_image

    def __get_k8s_container_name(self, job_wrapper):
        # TODO check if this is correct
        return job_wrapper.job_destination.id
   ###############################################################################################################
    ####Response {:orig-content-encoding nil,
    #              :request-time 121
     #             :status 400
       #           :headers {"Server" "Jetty(8.y.z-SNAPSHOT"
       #                     "Connection" "close"
       #                     "Content-Length" "0"
       #                     "Content-Type" "text/html;charset=ISO-8859-1"
       #                     "Cache-Control" "must-revalidate,no-cache,no-store"}
       #         :body ""}
    ####
    def check_watched_item(self, job_state):
        """Checks the state of a job already submitted on Gomesos. Job state is a AsynchronousJobState"""
         gomesos_job_name=self.__produce_gomesos_job_name(job_state.job_wrapper.get_id_tag())
         json_job_data_resp=self.chronos_cli.list(gomesos_job_name)
         
         response=json_job_data_resp.json()

        if len(response['successCount'] == 1:
            #avvisa Chronos del successo ( per job asincroni con una PUT)
            job = Job(self._pykube_api, jobs.response['items'][0])
            job_destination = job_state.job_wrapper.job_destination
            succeeded = 0
            active = 0
            failed = 0

            max_pod_retrials = 1
            if 'k8s_pod_retrials' in self.runner_params:
                max_pod_retrials = int(self.runner_params['k8s_pod_retrials'])
            if 'max_pod_retrials' in job_destination.params:
                max_pod_retrials = int(job_destination.params['max_pod_retrials'])

            if 'succeeded' in job.obj['status']:
                succeeded = job.obj['status']['succeeded']
            if 'active' in job.obj['status']:
                active = job.obj['status']['active']
            if 'failed' in job.obj['status']:
                failed = job.obj['status']['failed']

            # This assumes jobs dependent on a single pod, single container
            if succeeded > 0:
                self.__produce_log_file(job_state)
                error_file = open(job_state.error_file, 'w')
                error_file.write("")
                error_file.close()
                job_state.running = False
                self.mark_as_finished(job_state)
                return None
            elif active > 0 and failed <= max_pod_retrials:
                job_state.running = True
                return job_state
            elif failed > max_pod_retrials:
                self.__produce_log_file(job_state)
                error_file = open(job_state.error_file, 'w')
                error_file.write("Exceeded max number of Kubernetes pod retrials allowed for job\n")
                error_file.close()
                job_state.running = False
                job_state.fail_message = "More pods failed than allowed. See stdout for pods details."
                self.mark_as_failed(job_state)
                job.scale(replicas=0)
                return None

            # We should not get here
            log.debug(
                "Reaching unexpected point for Kubernetes job, where it is not classified as succ., active nor failed.")
            return job_state

        elif len(jobs.response['items']) == 0:
            # there is no job responding to this job_id, it is either lost or something happened.
            log.error("No Jobs are available under expected selector app=" + job_state.job_id)
            error_file = open(job_state.error_file, 'w')
            error_file.write("No Kubernetes Jobs are available under expected selector app=" + job_state.job_id + "\n")
            error_file.close()
            self.mark_as_failed(job_state)
            return job_state
        else:
            # there is more than one job associated to the expected unique job id used as selector.
            log.error("There is more than one Kubernetes Job associated to job id " + job_state.job_id)
            self.__produce_log_file(job_state)
            error_file = open(job_state.error_file, 'w')
            error_file.write("There is more than one Kubernetes Job associated to job id " + job_state.job_id + "\n")
            error_file.close()
            self.mark_as_failed(job_state)
            return job_state

    def fail_job(self, job_state):
        """
        Kubernetes runner overrides fail_job (called by mark_as_failed) to rescue the pod's log files which are left as
        stdout (pods logs are the natural stdout and stderr of the running processes inside the pods) and are
        deleted in the parent implementation as part of the failing the job process.

        :param job_state:
        :return:
        """

        # First we rescue the pods logs
        with open(job_state.output_file, 'r') as outfile:
            stdout_content = outfile.read()

        if getattr(job_state, 'stop_job', True):
            self.stop_job(self.sa_session.query(self.app.model.Job).get(job_state.job_wrapper.job_id))
        self._handle_runner_state('failure', job_state)
        # Not convinced this is the best way to indicate this state, but
        # something necessary
        if not job_state.runner_state_handled:
            job_state.job_wrapper.fail(
                message=getattr(job_state, 'fail_message', 'Job failed'),
                stdout=stdout_content, stderr='See stdout for pod\'s stderr.'
            )
            if job_state.job_wrapper.cleanup_job == "always":
                job_state.cleanup()

    def __produce_log_file(self, job_state):
        pod_r = Pod.objects(self._pykube_api).filter(selector="app=" + job_state.job_id)
        logs = ""
        for pod_obj in pod_r.response['items']:
            try:
                pod = Pod(self._pykube_api, pod_obj)
                logs += "\n\n==== Pod " + pod.name + " log start ====\n\n"
                logs += pod.logs(timestamps=True)
                logs += "\n\n==== Pod " + pod.name + " log end   ===="
            except Exception as detail:
                log.info("Could not write pod\'s " + pod_obj['metadata']['name'] +
                         " log file due to HTTPError " + str(detail))

        logs_file_path = job_state.output_file
        logs_file = open(logs_file_path, mode="w")
        if isinstance(logs, text_type):
            logs = logs.encode('utf8')
        logs_file.write(logs)
        logs_file.close()
        return logs_file_path

    def stop_job(self, job):
        """Attempts to delete a dispatched job to the k8s cluster"""
        try:
            jobs = Job.objects(self._pykube_api).filter(selector="app=" +
                                                                 self.__produce_unique_k8s_job_name(job.get_id_tag()))
            if len(jobs.response['items']) >= 0:
                job_to_delete = Job(self._pykube_api, jobs.response['items'][0])
                job_to_delete.scale(replicas=0)
            # TODO assert whether job parallelism == 0
            # assert not job_to_delete.exists(), "Could not delete job,"+job.job_runner_external_id+" it still exists"
            log.debug("(%s/%s) Terminated at user's request" % (job.id, job.job_runner_external_id))
        except Exception as e:
            log.debug("(%s/%s) User killed running job, but error encountered during termination: %s" % (
                job.id, job.job_runner_external_id, e))

    def recover(self, job, job_wrapper):
        """Recovers jobs stuck in the queued/running state when Galaxy started"""
        # TODO this needs to be implemented to override unimplemented base method
        job_id = job.get_job_runner_external_id()
        if job_id is None:
            self.put(job_wrapper)
            return
        ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper)
        ajs.job_id = str(job_id)
        ajs.command_line = job.command_line
        ajs.job_wrapper = job_wrapper
        ajs.job_destination = job_wrapper.job_destination
        if job.state == model.Job.states.RUNNING:
            log.debug("(%s/%s) is still in running state, adding to the runner monitor queue" % (
                job.id, job.job_runner_external_id))
            ajs.old_state = model.Job.states.RUNNING
            ajs.running = True
            self.monitor_queue.put(ajs)
        elif job.state == model.Job.states.QUEUED:
            log.debug("(%s/%s) is still in queued state, adding to the runner monitor queue" % (
                job.id, job.job_runner_external_id))
            ajs.old_state = model.Job.states.QUEUED
            ajs.running = False
            self.monitor_queue.put(ajs)

     
   class ChronosJob(object):
    fields = [
        "async",
        "command",
        "epsilon",
        "name",
        "owner",
        "disabled"
    ]
    one_of = ["schedule", "parents"]
    container_fields = [
        "type",
        "image"
    ]


      def connect(servers, proto="https", username="admin", password="m3sosGalaxyC.2016"):
           assert client.servers == ['https://172.30.67.7:4443'] ####mesos-m0
           return ChronosClient(servers, proto=proto, username=username, password=password)
