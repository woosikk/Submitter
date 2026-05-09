# submit.py


from __future__ import print_function

__doc__ = """Submit jobs processes."""

import os
import re
import socket
import sys
import subprocess
import time

import numpy as np



class Submitter (object):

    """Submit jobs as processes."""

    def __init__ (self,
            job_dir='jobs/', 
            dry=False, max_jobs=None, delay=0, memory=None, ncpu=None, 
            config='.bashrc_condor',
            logfile=sys.stderr):
        """Construct a Submitter."""
        self.job_dir = job_dir
        self.dry = dry
        self.max_jobs = max_jobs
        self.memory = memory
        self.ncpu = ncpu
        self.delay = delay
        self.config = config
    @property
    def dry (self):
        """Whether submit should do dry runs, not actually submit jobs."""
        return self._dry

    @dry.setter
    def dry (self, value):
        self._dry = value

    @property
    def max_jobs (self):
        """The maximum number of jobs to process at once."""
        return self._max_jobs

    @max_jobs.setter
    def max_jobs (self, max_jobs):
        self._max_jobs = max_jobs
    
    @property
    def delay (self):
        """Number of seconds to wait between submitting jobs."""
        return self._delay

    @delay.setter
    def delay (self, delay):
        self._delay = delay

    @property
    def job_dir (self):
        """The directory in which to store job scripts and output."""
        return self._job_dir

    @job_dir.setter
    def job_dir (self, job_dir):
        self._job_dir = job_dir

    @property
    def ncpu (self):
        """The amount of cpu's requested (for cluster submission)."""
        return self._ncpu

    @ncpu.setter
    def ncpu (self, ncpu):
        """The amount of cpu's requested (for cluster submission)."""
        self._ncpu = ncpu

    @property
    def memory (self):
        """The amount of memory requested in GB (for cluster submission)."""
        return self._memory

    @memory.setter
    def memory (self, value_in_GB):
        self._memory = value_in_GB

    @property
    def config (self):
        """Config file under $HOME (default: .bashrc_condor)"""
        return self._config

    @config.setter
    def config (self, filename):
        self._config = filename

    def announce_command (self, cmd):
        if self.dry:
            self.log ('***** Would execute command:')
        self.log ('$ ' + cmd)
        self.log ()

    def submit_serial (self, commands, command_labels):
        """Submit jobs sequentially."""
        for command in commands:
            self.announce_command (command)
            if not self.dry:
                os.system (command)

    def submit_threads (self, commands, command_labels):
        """Submit jobs in parallel on the current host."""
        ensure_dir (self.job_dir)
        import shlex
        import subprocess
        procs = []
        def n_running ():
            polls = [proc.poll() for proc in procs]
            returncodes = [proc.returncode for proc in procs]
            running = [returncode is None for returncode in returncodes]
            return np.sum (running)

        def too_many ():
            return self.max_jobs \
                    and n_running () >= self.max_jobs

        n_total = len (commands)
        length = len (str (n_total))
        fmt =  '{0}/threads-{1}.out'

        for n, (command, label) in enumerate (zip (commands, command_labels)):
            stdout_filename = os.path.join (
                    self.job_dir, 'threads_{0}.out'.format (label))
            stderr_filename = stdout_filename[:-3] + 'err'
            stdout = open (stdout_filename, 'w')
            stderr = open (stderr_filename, 'w')
            args = shlex.split (command)
            self.announce_command (command)
            if not self.dry:
                if too_many ():
                    s = Spinner ()
                    self.log ('waiting for available thread... ', end='')
                    s.start ()
                    time.sleep (2)
                    while too_many ():
                        time.sleep (2)
                        s.next ()
                    s.finish ()
                    self.log ('submitting now.')
                    self.log ()
                proc = subprocess.Popen (args, stdout=stdout, stderr=stderr)
                procs.append (proc)
                if self.delay:
                    time.sleep (self.delay)

        s = Spinner ()
        self.log ('waiting for threads to finish... ', end='')
        s.start ()
        while n_running () > 0:
            time.sleep (2)
            s.next ()
        s.finish ()
        self.log ('threads finished.')

    def submit_cobol00 (self, commands, command_labels, username=None):
        """Submit jobs in parallel on the cobol00 SGE cluster.

        This method logs into pa-pub, then into cobol00.  There, it executes
        the given command(s) on the cluster with qsub.

        `commands`: a sequence of commands, or a single command.
        `command_labels`: a sequence of command labels, or a single one.
        `username`: the username in use on cobol00.
        """
        if len (commands) == 0:
            print ('warning: no jobs')
            return
        job_dir = os.path.realpath (ensure_dir (self.job_dir))
        if isinstance (commands, str):
            commands = [commands]
        if isinstance (command_labels, str):
            command_labels = [command_labels]
        n_total = len (commands)
        length = len (str (n_total))

        subscript_filename = os.path.join (
                job_dir, 'cobol00_qsub.sh')
        subscript = open (subscript_filename, 'w')
        def spr (*args, **kwargs):
            print (*args, file=subscript, **kwargs)

        spr ('. /data/sge/current/icecube/common/settings.sh')
        os.system ('touch {0}/placeholder.o {0}/placeholder.queue'.format (
            job_dir))
        print ('Submitting jobs from {0} ...'.format (job_dir))
        for n, (command, label) in enumerate (zip (commands, command_labels)):
            script_filename = os.path.join (
                    job_dir, 'cobol00_{0}.sh'.format (label))
            qsub_command = 'qsub -q all.q -e {0} -o {0} {1}'.format (
                    os.path.realpath (job_dir),
                    os.path.realpath (script_filename))
            with open (script_filename, 'w') as script:
                def pr (*args, **kwargs):
                    print (*args, file=script, **kwargs)

                pr ('#!/bin/sh')
                pr ('#$ -S /bin/sh')
                pr ()
                pr ('# {0}'.format (qsub_command))
                if self.memory:
                    pr ('#$ -l h_vmem={0:.2f}G'.format (self.memory))
                pr ()
                pr ('. $HOME/.bashrc_sge')
                pr ()
                pr ('hostname')
                pr ()
                pr ('before=`date +%s`')
                pr ('echo Begin: `date`.')
                pr ('echo')
                pr ()
                pr (command)
                pr ('result=$?')
                pr ()
                pr ('echo')
                pr ('after=`date +%s`')
                pr ('echo End: `date`.')
                pr ()
                pr ('exit $result')

            os.chmod (script_filename, 0o775)
            user_str = username + '@' if username else ''
            q_note_command = 'touch {0}.queue'.format (script_filename)
            spr (q_note_command)
            if self.max_jobs and n >= 1:
                # wait_cmd = 'while test `qstat|grep {0}|wc -l` -ge {1}'.format (
                #         username, self.max_jobs) \
                #             + '; do sleep 10; done'
                wait_cmd = "while test " \
                    "$(expr `ls {0}/*.queue | wc -l` " \
                    "- `tail -n1 {0}/*.o* | grep '^End: ' | wc -l`) " \
                    "-ge {1}".format (job_dir, self.max_jobs + 2) \
                            + '; do sleep 10; done'
                spr (wait_cmd)
            spr (qsub_command)
            if self.delay:
                spr ('sleep {0:.0f}'.format (self.delay))
        subscript.close ()
        hostname = socket.gethostname ()
        subscript_path = os.path.realpath (subscript_filename)
        if hostname == 'cobol00':
            qsub_command = '. {0}'.format (subscript_path)
        else:
            qsub_command = 'ssh {0}pa-pub.umd.edu "ssh cobol00 ' \
                    '\'source {1} \' "'.format (
                            user_str, subscript_path)

        if not self.dry:
            os.system (qsub_command)
        else:
            self.log (qsub_command)

    def submit_condor00 (self, commands, command_labels,
            username=None,
            blacklist=[],
            reqs=None,
            max_per_interval=None,
            ):
        """Submit jobs in parallel on the condor00 Condor cluster.

        This method logs into pa-pub, then into condor00.  There, it executes
        the given command(s) on the cluster with qsub.

        `commands`: a sequence of commands, or a single command.
        `command_labels`: a sequence of command labels, or a single one.
        `username`: the username in use on condor00.
        `blacklist`: a list of hosts to avoid
        """
        if len (commands) == 0:
            print ('warning: no jobs')
            return
        if len (set (command_labels)) != len (command_labels):
            raise ValueError (
                '`command_labels` must not include duplicate labels')
        job_dir = os.path.realpath (ensure_dir (self.job_dir))
        log_dir = ensure_dir(os.path.join(job_dir, 'logs') )
        if isinstance (commands, str):
            commands = [commands]
        if isinstance (command_labels, str):
            command_labels = [command_labels]
        n_total = len (commands)
        length = len (str (n_total))

        subdag_filename = os.path.realpath ((os.path.join (
                job_dir, 'condor00_submit.dag')))
        subdag_config_filename = os.path.realpath ((os.path.join (
                job_dir, 'condor00_submit.dag.config')))
        subdag = open (subdag_filename, 'w')
        subdag_config = open (subdag_config_filename, 'w')

        def spr_dag (*args, **kwargs):
            print (*args, file=subdag, **kwargs)

        def spr_dag_config (*args, **kwargs):
            print (*args, file=subdag_config, **kwargs)

        spr_dag ('CONFIG {0}'.format (subdag_config_filename))
        if max_per_interval:
            spr_dag_config (
                    'DAGMAN_MAX_SUBMITS_PER_INTERVAL =',
                    max_per_interval)

        for n, (command, label) in enumerate (zip (commands, command_labels)):
            dag_label = 'condor00_{0}.sh'.format (label)
            dag_label = re.sub (r'\.', '_dot_', dag_label)
            dag_label = re.sub (r'\+', '_plus_', dag_label)
            dag_label = re.sub (r'-', '_minus_', dag_label)
            script_filename = os.path.realpath (os.path.join (
                    log_dir, dag_label))
            #script_filename = os.path.realpath (os.path.join (
            #        job_dir, 'condor00_{0}.sh'.format (label)))
            with open (script_filename, 'w') as script:
                def pr (*args, **kwargs):
                    print (*args, file=script, **kwargs)

                pr ('#!/bin/sh')
                pr ('#$ -S /bin/sh')
                pr ()
                pr ('')
                pr ()
                pr ('. {0}/{1}'.format (os.getenv ('HOME'), self.config))
                pr ()
                pr ('hostname')
                pr ()
                pr ('before=`date +%s`')
                pr ('echo Begin: `date`.')
                pr ('echo')
                pr ()
                pr (command)
                pr ('result=$?')
                pr ()
                pr ('echo')
                pr ('after=`date +%s`')
                pr ('echo End: `date`.')
                pr ()
                pr ('exit $result')

            os.chmod (script_filename, 0o775)

            tosubsub_filename = script_filename + '.sub'
            with open (tosubsub_filename, 'w') as tosubsub:
                def pr (*args, **kwargs):
                    print (*args, file=tosubsub, **kwargs)

                pr ('Universe       = vanilla')
                pr ('Executable     = {0}'.format (script_filename))
                pr ('Log            = {}/{}.log'.format (log_dir, dag_label))
                pr ('Output         = {}/{}.out'.format (log_dir, dag_label))
                pr ('Error          = {}/{}.err'.format (log_dir, dag_label))
                pr ('Notification   = NEVER')
                if blacklist:
                    reqs_bl = ' && '.join (
                            ['(Machine != "{0}")'.format (host)
                                for host in blacklist])
                    if reqs:
                        pr('Requirements = {} && {}'.format(reqs, reqs_bl))
                    else:    
                        pr('Requirements = {}'.format(reqs_bl))
                else:
                    if reqs:
                        pr('Requirements = {}'.format(reqs))
                if self.memory:
                    pr ('request_memory = {0:.2f}G'.format (self.memory))
                if self.ncpu:
                    pr ('request_cpus = {0:.0f}'.format (self.ncpu))
                pr ('Queue')

            user_str = username + '@' if username else ''
            dag_command = 'JOB {0} {1}'.format (
                os.path.basename (script_filename), tosubsub_filename)
            spr_dag (dag_command)

        subdag.close ()
        subdag_config.close ()
        hostname = socket.gethostname ()

        if 'condor' in hostname:
            if self.max_jobs:
                condor00_command = 'condor_submit_dag -maxjobs {0} {1}'.format (
                    self.max_jobs, os.path.realpath (subdag_filename))
            else:
                condor00_command = 'condor_submit_dag {0}'.format (
                    os.path.realpath (subdag_filename))
        else:
            if self.max_jobs:
                condor00_command = 'ssh {0}pa-pub.umd.edu "ssh condor00 ' \
                        '\'condor_submit_dag -maxjobs {1} {2}\' "'.format (
                            user_str, self.max_jobs, self.max_jobs,
                            os.path.realpath (subdag_filename))
            else:
                condor00_command = 'ssh {0}pa-pub.umd.edu "ssh condor00 ' \
                        '\'condor_submit_dag {1}\' "'.format (
                            user_str,
                            os.path.realpath (subdag_filename))
        if not self.dry:
            print ('Submitting {} jobs\nfrom {} .'.format (n_total, job_dir))
            os.system (condor00_command)
        else:
            print ('Prepared {} jobs\n in {} .'.format (n_total, job_dir))
            self.log (condor00_command)

    def submit_npx (self, commands, command_labels,
                     username=None, reqs=None,
                     blacklist=[], gpus = None):
        """Submit jobs in parallel on the npx Condor cluster.

        This method logs into pub.icecube.wisc.edu, then into npx.  There, it
        executes the given command(s) on the cluster with condor_submit.

        `commands`: a sequence of commands, or a single command.
        `command_labels`: a sequence of command labels, or a single one.
        `username`: the username in use on npx.
        """
        if len (set (command_labels)) != len (command_labels):
            raise ValueError (
                '`command_labels` must not include duplicate labels')
        job_dir = os.path.realpath (ensure_dir (self.job_dir))
        log_dir = ensure_dir(os.path.join(job_dir, 'logs') )
        if isinstance (commands, str):
            commands = [commands]
        if isinstance (command_labels, str):
            command_labels = [command_labels]
        n_total = len (commands)
        length = len (str (n_total))

        subdag_filename = os.path.realpath ((os.path.join (
                job_dir, 'npx_submit.dag')))
        subdag_config_filename = os.path.realpath ((os.path.join (
                job_dir, 'npx_submit.dag.config')))
        subdag = open (subdag_filename, 'w')
        subdag_config = open (subdag_config_filename, 'w')

        def spr_dag (*args, **kwargs):
            print (*args, file=subdag, **kwargs)
        def spr_dag_config (*args, **kwargs):
            print (*args, file=subdag_config, **kwargs)
        spr_dag ('CONFIG {0}'.format (subdag_config_filename))
        spr_dag_config ('DAGMAN_MAX_SUBMITS_PER_INTERVAL = 50')

        for n, (command, label) in enumerate (zip (commands, command_labels)):
            dag_label = 'npx_{0}.sh'.format (label)
            dag_label = re.sub (r'\.', '_dot_', dag_label)
            dag_label = re.sub (r'\+', '_plus_', dag_label)
            dag_label = re.sub (r'-', '_minus_', dag_label)
            script_filename = os.path.realpath (os.path.join (
                    log_dir, dag_label))
            with open (script_filename, 'w') as script:
                def pr (*args, **kwargs):
                    print (*args, file=script, **kwargs)
                pr ('#!/bin/sh')
                pr ('#$ -S /bin/sh')
                pr ()
                pr ('')
                pr ()
                pr ('. {0}/{1}'.format (os.getenv ('HOME'), self.config))
                pr ()
                pr ('hostname')
                pr ()
                pr ('before=`date +%s`')
                pr ('echo Begin: `date`.')
                pr ('echo')
                pr ()
                pr (command)
                pr ('result=$?')
                pr ()
                pr ('echo')
                pr ('after=`date +%s`')
                pr ('echo End: `date`.')
                pr ()
                pr ('exit $result')
            hostname = socket.gethostname ()
            out_filename = script_filename + '.out'
            #subprocess.call ('touch {}'.format( out_filename), shell=True)
            #os.chmod (out_filename, 0o775)
            os.chmod (script_filename, 0o775)
            tosubsub_filename = script_filename + '.sub'
            with open (tosubsub_filename, 'w') as tosubsub:
                def pr (*args, **kwargs):
                    print (*args, file=tosubsub, **kwargs)

                pr ('Universe       = vanilla')
                pr ('Executable     = {0}'.format (script_filename))
                pr ('Log            = {}/{}.log'.format (log_dir, dag_label))
                pr ('Output         = {}/{}.out'.format (log_dir, dag_label))
                pr ('Error          = {}/{}.err'.format (log_dir, dag_label))
                pr ('Notification   = NEVER')
                if 'submit-1' in hostname:
                    pr ('should_transfer_files = YES')
                    #pr ('when_to_transfer_output = ON_EXIT')
                    pr ('stream_output = True')
                if blacklist:
                    reqs_bl = ' && '.join (
                            ['(Machine != "{0}")'.format (host)
                                for host in blacklist])
                    if reqs:
                        pr('Requirements = {} && {}'.format(reqs, reqs_bl))
                    else:    
                        pr('Requirements = {}'.format(reqs_bl))
                else:
                    if reqs:
                        pr('Requirements = {}'.format(reqs))
                if gpus:
                    pr ('request_gpus = 1')

                if self.memory:
                    pr ('request_memory = {0:.2f}G'.format (self.memory))
                if self.ncpu:
                    pr ('request_cpus = {0:.0f}'.format (self.ncpu))
                pr ('Queue')

            user_str = username + '@' if username else ''
            dag_command = 'JOB {0} {1}'.format (
                os.path.basename (script_filename), tosubsub_filename)
            spr_dag (dag_command)

        subdag.close ()
        subdag_config.close ()

        print ('Submitting jobs from {0} ...'.format (job_dir))
        if 'npx-submitter' in hostname:
            if self.max_jobs:
                npx_command = 'condor_submit_dag -maxjobs {0} {1}'.format (
                    self.max_jobs, os.path.realpath (subdag_filename))
            else:
                npx_command = 'condor_submit_dag {0}'.format (
                    os.path.realpath (subdag_filename))
        elif 'cobalt' in hostname:
            if self.max_jobs:
                npx_command = 'ssh npx-submitter "condor_submit_dag -maxjobs {0} {1}"'.format (
                    self.max_jobs, os.path.realpath (subdag_filename))
            else:
                npx_command = 'ssh npx-submitter "condor_submit_dag {0}"'.format (
                    os.path.realpath (subdag_filename))
        else:
            if self.max_jobs:
                npx_command = 'ssh {0}pub.icecube.wisc.edu "ssh npx-submitter ' \
                        '\'condor_submit_dag -maxjobs {1} {2}\' "'.format (
                            user_str, self.max_jobs, self.max_jobs,
                            os.path.realpath (subdag_filename))
            else:
                npx_command = 'ssh {0}pub.icecube.wisc.edu "ssh npx-submitter ' \
                        '\'condor_submit_dag {1}\' "'.format (
                            user_str,
                            os.path.realpath (subdag_filename))
        if not self.dry:
            print ('Submitting {0} jobs.'.format (n_total))
            os.system (npx_command)
        else:
            print ('Prepared {0} jobs.'.format (n_total))
            self.log (npx_command)

    def submit_osg (self, commands, command_labels,
                    transfers='',
                    reqs = None,
                    username=None,
                    userid=None):
        """Submit jobs in parallel on the OSG Condor cluster.

        This method creates the job files in a temporary job_dir initialized
        with the submitter. The job dir is rsync'd to sub-1 in the job path
        /scratch/<username>/jobs. It then ssh's into sub-1 and there, it
        executes the given command(s) on the cluster with condor_submit_dag. It
        is assumed a grid proxy has already been initialized on sub-1.

        `commands`: a sequence of commands, or a single command.
        `command_labels`: a sequence of command labels, or a single one.
        `transfers`: files on sub-1 to transfer to grid when running job
        `username`: the username in use on sub-1
        `userid`: the userid in use for grid certification
        """

        job_dir = os.path.realpath (ensure_dir (self.job_dir))
        print ('Temporary job directory: {0}'.format (job_dir))

        if isinstance (commands, str):
            commands = [commands]
        if isinstance (command_labels, str):
            command_labels = [command_labels]
        n_total = len (commands)
        length = len (str (n_total))

        # get username, user id if not given
        if username is None:
            username = os.getenv ('USER')
        if userid is None:
            userid = int (os.popen ('id -u {0}'.format (username)).read ())

        subdag_filename = os.path.realpath ((os.path.join (
                job_dir, 'osg_submit.dag')))
        subdag_config_filename = os.path.realpath ((os.path.join (
                job_dir, 'osg_submit.dag.config')))
        subdag = open (subdag_filename, 'w')
        subdag_config = open (subdag_config_filename, 'w')

        def spr_dag (*args, **kwargs):
            print (*args, file=subdag, **kwargs)
        def spr_dag_config (*args, **kwargs):
            print (*args, file=subdag_config, **kwargs)
        spr_dag ('CONFIG {0}'.format (os.path.basename (subdag_config_filename)))
        spr_dag_config ('DAGMAN_MAX_SUBMITS_PER_INTERVAL = 50')

        for n, (command, label) in enumerate (zip (
                commands, command_labels)):
            script_filename = os.path.realpath (os.path.join (
                    job_dir, 'osg_{0}.sh'.format (label)))
            with open (script_filename, 'w') as script:
                def pr (*args, **kwargs):
                    print (*args, file=script, **kwargs)

                pr ('#!/bin/sh')
                pr ('#$ -S /bin/sh')
                pr ()
                pr ('hostname')
                pr ()
                pr ('before=`date +%s`')
                pr ('echo Begin: `date`.')
                pr ('echo')
                pr ()
                pr (command)
                pr ('result=$?')
                pr ()
                pr ('echo')
                pr ('after=`date +%s`')
                pr ('echo End: `date`.')
                pr ()
                pr ('exit $result')

            os.chmod (script_filename, 0o775)

            tosubsub_filename = script_filename + '.sub'
            with open (tosubsub_filename, 'w') as tosubsub:
                def pr (*args, **kwargs):
                    print (*args, file=tosubsub, **kwargs)

                pr ('Executable     = {0}'.format (os.path.basename (script_filename)))
                pr ('Log            = {0}.log'.format (os.path.basename (script_filename)))
                pr ('Output         = {0}.out'.format (os.path.basename (script_filename)))
                pr ('Error          = {0}.err'.format (os.path.basename (script_filename)))
                pr ('Environment    = "X509_USER_PROXY=x509up_u{0}"'.format (userid))
                if transfers:
                    pr ('transfer_input_files = /tmp/x509up_u{0},{1}'.format (userid, transfers))
                else:
                    pr ('transfer_input_files = /tmp/x509up_u{0}'.format (userid))
                pr ('+TransferOutput=""')
                pr ('Universe       = vanilla')
                pr ('Notification   = never')
                pr ('+WantRHEL6     = True')
                pr ('+WantGlideIn   = True')
                default_reqs = ( 
                    '( Arch == "X86_64" ) && ( TARGET.OpSys == "LINUX" ) && ' +
                    '( OASIS_CVMFS_Exists =?= True || (IS_GLIDEIN && HasParrotCVMFS   ' +
                    ' && GLIDEIN_Site != "UNESP"      ' +
                    ' && GLIDEIN_Site != "UConn"      ' +
                    ' && GLIDEIN_Site != "Cornell"    ' )
                if reqs:
                    pr('Requirements = {} && {}'.format(reqs, default_reqs))
                else:
                    #TODO: check if the requirements below are still relavant?  
                    pr ('Requirements   = ( Arch == "X86_64" ) &&               \\')
                    pr ('                 ( TARGET.OpSys == "LINUX" ) &&        \\')
                    pr ('                 ( OASIS_CVMFS_Exists =?= True ||      \\')
                    pr ('                       (IS_GLIDEIN && HasParrotCVMFS   \\')
                    pr ('                       && GLIDEIN_Site != "UNESP"      \\')
                    pr ('                       && GLIDEIN_Site != "UConn"      \\')
                    pr ('                       && GLIDEIN_Site != "Cornell"    \\')
                    pr ('                 ))')
                if self.memory:
                    pr ('request_memory = {0:.2f}G'.format (self.memory))
                # if TEST:
                #     pr ('+IsTestQueue   = TRUE')
                #     pr ('requirements	= TARGET.IsTestQueue')
                pr ('queue')

            user_str = username + '@' if username else ''
            dag_command = 'JOB {0} {1}'.format (label, os.path.basename (tosubsub_filename))
            spr_dag (dag_command)

        subdag.close ()
        subdag_config.close ()
        hostname = socket.gethostname ()

        rsync_command = 'rsync -paq {0} {1}@sub-1:/scratch/{1}/jobs'.format (
            job_dir, username)

        # Submitting the OSG dagman assumes a OSG proxy session is already
        # initialized on sub-1
        if self.max_jobs:
            osg_command = 'ssh sub-1 "cd /scratch/{0}/jobs/{1} && condor_submit_dag -maxjobs {2} osg_submit.dag"'.format (
                username, os.path.basename (job_dir), self.max_jobs)
        else:
            osg_command = 'ssh sub-1 "cd /scratch/{0}/jobs/{1} && condor_submit_dag osg_submit.dag"'.format (
                username, os.path.basename (job_dir))

        print (osg_command)

        if not self.dry:
            print ('Moving {0} jobs to {1}@sub-1.icecube.wisc.edu:/scratch/{1}/jobs/{2}'.format (
                n_total, username, os.path.basename (job_dir)))
            os.system (rsync_command)
            print ('Submitting {0} jobs from {1}@sub-1.icecube.wisc.edu:/scratch/{1}/jobs/{2}'.format (
                n_total, username, os.path.basename (job_dir)))
            os.system (osg_command)
        else:
            print ('Prepared {0} jobs.'.format (n_total))
            self.log (osg_command)

    def log (self, *a, **kw):
        kw['file'] = sys.stderr
        print (*a, **kw)


    def log (self, *a, **kw):
        kw['file'] = sys.stderr
        print (*a, **kw)

    def submit_illume (self, commands, command_labels,
            username=None,
            blacklist=[],
            reqs=None,
            gpus=None,
            singularity=None,
            max_per_interval=None,
            ):
        """Submit jobs in parallel on illume Condor cluster.


        `commands`: a sequence of commands, or a single command.
        `command_labels`: a sequence of command labels, or a single one.
        `username`: the username in use on condor00.
        `blacklist`: a list of hosts to avoid
        """
        if len (commands) == 0:
            print ('warning: no jobs')
            return
        if len (set (command_labels)) != len (command_labels):
            raise ValueError (
                '`command_labels` must not include duplicate labels')
        job_dir = os.path.realpath (ensure_dir (self.job_dir))
        log_dir = ensure_dir(os.path.join(job_dir, 'logs') )
        if isinstance (commands, str):
            commands = [commands]
        if isinstance (command_labels, str):
            command_labels = [command_labels]
        n_total = len (commands)
        length = len (str (n_total))

        subdag_filename = os.path.realpath ((os.path.join (
                job_dir, 'illume_submit.dag')))
        subdag_config_filename = os.path.realpath ((os.path.join (
                job_dir, 'illume_submit.dag.config')))
        subdag = open (subdag_filename, 'w')
        subdag_config = open (subdag_config_filename, 'w')

        def spr_dag (*args, **kwargs):
            print (*args, file=subdag, **kwargs)

        def spr_dag_config (*args, **kwargs):
            print (*args, file=subdag_config, **kwargs)

        spr_dag ('CONFIG {0}'.format (subdag_config_filename))
        if max_per_interval:
            spr_dag_config (
                    'DAGMAN_MAX_SUBMITS_PER_INTERVAL =',
                    max_per_interval)

        for n, (command, label) in enumerate (zip (commands, command_labels)):
            dag_label = 'illume_{0}.sh'.format (label)
            dag_label = re.sub (r'\.', '_dot_', dag_label)
            dag_label = re.sub (r'\+', '_plus_', dag_label)
            dag_label = re.sub (r'-', '_minus_', dag_label)
            script_filename = os.path.realpath (os.path.join (
                    log_dir, dag_label))
            #script_filename = os.path.realpath (os.path.join (
            #        job_dir, 'condor00_{0}.sh'.format (label)))
            with open (script_filename, 'w') as script:
                def pr (*args, **kwargs):
                    print (*args, file=script, **kwargs)

                pr ('#!/bin/sh')
                pr ('#$ -S /bin/sh')
                pr ()
                pr ('')
                pr ()
                pr ('. {0}/{1}'.format (os.getenv ('HOME'), self.config))
                pr ()
                pr ('hostname')
                pr ()
                pr ('before=`date +%s`')
                pr ('echo Begin: `date`.')
                pr ('echo')
                pr ()
                pr (command)
                pr ('result=$?')
                pr ()
                pr ('echo')
                pr ('after=`date +%s`')
                pr ('echo End: `date`.')
                pr ()
                pr ('exit $result')

            os.chmod (script_filename, 0o775)

            tosubsub_filename = script_filename + '.sub'
            with open (tosubsub_filename, 'w') as tosubsub:
                def pr (*args, **kwargs):
                    print (*args, file=tosubsub, **kwargs)

                pr ('Universe       = vanilla')
                pr ('Executable     = {0}'.format (script_filename))
                pr ('Log            = {}/{}.log'.format (log_dir, dag_label))
                pr ('Output         = {}/{}.out'.format (log_dir, dag_label))
                pr ('Error          = {}/{}.err'.format (log_dir, dag_label))
                if singularity:
                    pr ('+SingularityImage = "{}"'.format(singularity))
                    pr ('requirements = HasSingularity')
                if gpus:
                    pr ('request_gpus = 1')
                pr ('Notification   = NEVER')

                if blacklist:
                    reqs_bl = ' && '.join (
                            ['(Machine != "{0}")'.format (host)
                                for host in blacklist])
                    if reqs:
                        pr('Requirements = {} && {}'.format(reqs, reqs_bl))
                    else:    
                        pr('Requirements = {}'.format(reqs_bl))
                else:
                    if reqs:
                        pr('Requirements = {}'.format(reqs))
                if self.memory:
                    pr ('request_memory = {0:.2f}GB'.format (self.memory))
                if self.ncpu:
                    pr ('request_cpus = {0:.0f}'.format (self.ncpu))
                else:
                    pr ('request_cpus = 1')
                pr ('Queue')

            user_str = username + '@' if username else ''
            dag_command = 'JOB {0} {1}'.format (
                os.path.basename (script_filename), tosubsub_filename)
            spr_dag (dag_command)

        subdag.close ()
        subdag_config.close ()
        hostname = socket.gethostname ()

        if 'illume' in hostname:
            if self.max_jobs:
                condor00_command = 'condor_submit_dag -maxjobs {0} {1}'.format (
                    self.max_jobs, os.path.realpath (subdag_filename))
            else:
                condor00_command = 'condor_submit_dag {0}'.format (
                    os.path.realpath (subdag_filename))
        if not self.dry:
            print ('Submitting {} jobs\nfrom {} .'.format (n_total, job_dir))
            os.system (condor00_command)
        else:
            print ('Prepared {} jobs\n in {} .'.format (n_total, job_dir))
            self.log (condor00_command)
class Spinner (object):

    """Create a simple spinning progress indicator."""

    seq = ['-', '\\', '|', '/']
    N = len (seq)

    def __init__ (self, f=sys.stdout):
        """Initialize the Spinner, writing to file `f`."""
        self.i = 0
        self.f = f

    def start (self):
        """Start the Spinner."""
        self.f.write (self.cur)
        self.f.flush ()

    def next (self):
        """Rotate the Spinner."""
        self.i += 1
        self.f.write ('\b' + self.cur)
        self.f.flush ()

    def finish (self):
        """Clear the Spinner."""
        self.f.write ('\b  \b\b')
        self.f.flush ()

    @property
    def cur (self):
        """The current character of the Spinner's display."""
        return self.seq[self.i % self.N]

def ensure_dir (dirname):
    """Make sure ``dirname`` exists and is a directory."""
    if not os.path.isdir (dirname):
        try:
            os.makedirs (dirname)   # throws if exists as file
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
    return dirname

def gsiftp_wrapper (filename):
    return 'gsiftp://gridftp-users.icecube.wisc.edu{0}'.format (filename)


def on_cobol (submitter, func, *args, **kwargs):
    """Execute func(*args,**kwargs) on a cobol node.

    :type   submitter: :class:`Submitter` or None
    :param  submitter: A Submitter instance, if non-defaults are desired;
        otherwise None.

    :type   func: function
    :param  func: The function to run.

    :return: the name of the file in which the result will be located.

    This function assumes that the shell variable $IR4 is an IceTray
    environment script on the cobols.

    """
    import cache
    from misc import ensure_dir
    user = os.getenv ('USER')
    outdir = '/data/i3scratch0/users/{0}/on_cobol'.format (user)
    cachedir = ensure_dir ('{0}/.cache'.format (outdir))
    save_id = '{0}_nixtime_{2:.0f}_job_{1}'.format (
        socket.gethostname (), os.getpid (), time.time ())
    func_filename = '{0}/func_{1}.pickle'.format (cachedir, save_id)
    args_filename = '{0}/args_{1}.pickle'.format (cachedir, save_id)
    kwargs_filename = '{0}/kwargs_{1}.pickle'.format (cachedir, save_id)
    cache.save (func, func_filename)
    cache.save (args, args_filename)
    cache.save (kwargs, kwargs_filename)
    out_filename = '{0}/out_{1}.pickle'.format (outdir, save_id)

    jobdir = ensure_dir ('{0}/.jobs'.format (outdir))

    script_filename = '{0}/script_{1}.py'.format (jobdir, save_id)
    with open (script_filename, 'wt') as f:
        pr = lambda *a, **kw: print (*a, file=f, **kw)

        pr ('#!/usr/bin/env python')
        pr ('')
        pr ('from __future__ import print_function')
        pr ('from icecube.umdtools import cache')
        pr ('import os')
        pr ('')
        pr ('print ("Loading func...")')
        pr ('func = cache.load ("{0}")'.format (func_filename))
        pr ('print ("Loading args...")')
        pr ('args = cache.load ("{0}")'.format (args_filename))
        pr ('print ("Loading kwargs...")')
        pr ('kwargs = cache.load ("{0}")'.format (kwargs_filename))
        pr ('print ("Running function...")')
        pr ('result = func (*args, **kwargs)')
        pr ('print ("Saving {0} ...")'.format (out_filename))
        pr ('cache.save (result, "{0}")'.format (out_filename))
        pr ('os.remove ("{0}")'.format (func_filename))
        pr ('os.remove ("{0}")'.format (args_filename))
        pr ('os.remove ("{0}")'.format (kwargs_filename))

    command = '$IR4 python {0}'.format (script_filename)
    command_label = 'on_cobol_{0}'.format (save_id)

    if submitter is None:
        submitter = Submitter ()
    submitter.job_dir = jobdir

    submitter.submit_cobol00 ([command], [command_label])
    return out_filename
    


