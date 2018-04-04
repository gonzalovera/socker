import os,sys,subprocess,uuid
import pwd,grp

VERSION = "18.04"

class Socker:
    """Class keeping all the needed stuff used by socket"""

    def __init__( self ):
        # TODO: Define __init__ method and self objects
        # TODO: although... this is a singleton isn't?
        self.dockerusr = "dockerroot"
        self.dockergrp = "docker"
        self.socker_images_file = '/cluster/tmp/socker-images'
        self.msgErr_contact = 'hpc-drift@usit.uio.no\n'

        self.verbose = False

        self.cmd = None
        self.img = None
        self.dockerv = None
        self.dockeruid = None
        self.dockergid = None
        self.slurm_job_id = None

        self.user = None
        self.group = None
        self.PWD = None
        self.containerID = None
        self.home = None


    def initialize( self ):
        """Set the first values """
        # Get the UID and GID of the non-root user and group allowed to run docker
        try:
            self.dockeruid = pwd.getpwnam( self.dockerusr ).pw_uid
            self.dockergid = grp.getgrnam( self.dockergrp ).gr_gid
        except KeyError:
            print 'There must exist a user "'+ self.dockerusr +'" and a group "'+ self.dockergrp + '"'
            return False

        if not [g.gr_name for g in grp.getgrall() if self.dockerusr in g.gr_mem] == [ dockergrp ]:
            print 'The user "'+ self.dockerusr +'" must be a member of ONLY the "'+ self.dockergrp + '" group'
            return False
        
        # Get the current user information
        self.user = os.getuid()
        self.group = os.getgid()
        self.PWD = os.getcwd()
        self.containerID = str( uuid.uuid4() )
        self.home = pwd.getpwuid( self.user ).pw_dir

        try:
            self.slurm_job_id = os.environ['SLURM_JOB_ID']
            print 'Slurm job id', self.slurm_job_id
        except KeyError as e:
            #print e,slurm_job_id
            pass
        
        return True

    def becomeRoot():
        """Change the user and group running the process to root:root"""
        try:
            # Set the user to root
            os.setuid(0)
            os.setgid(0)
        except:
            print 'Unable to become root.'
            return False

        return True

    def getDockerVersion():
        """Run docker --version and keep result in dockerv"""
        p = subprocess.Popen('docker --version', \
                            shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out,err = p.communicate()

        #print 'return out and err',out,err
        if p.returncode !=0:
            print 'Docker is not found! Please verify that Docker is installed...'
            return False
        else:
            self.dockerv = out

        return True

    def safetyChecks( argv ):
        """Second phase initialization, checking values to ensure it is safe to run while building cmd string"""

        # Get and check the list of authorized images
        try:
            images = filter(None,[line.strip() for line in open( self.socker_images_file,'r')])
            if len(images) == 0:
                raise Exception()
        except:
            print 'No authorized images to run. Socker cannot be used at the moment.\nContact ' + self.msgErr_contact 
            return False

        if len(argv) < 2:
            print 'You need to specify an image to run'
            return False

        self.img = argv[1]
        if not self.img in images:
            if not 'ALL' in images: 
              print '"'+ self.img +'" is not an authorized image for this system. Please send a request to ' + self.msgErr_contact
            return False

        # Check commands==remainingOptions to be run inside the container
        if len(argv) >2:
            self.cmd = ''
            for nextOption in argv[2:]:
                if ' ' in nextOption or ';' in nextOption or '&' in nextOption:
                    # composite argument
                    nextOption = '"'+ nextOption +'"'
                    sys.stderr.write('WARNING: you have a composite argument '+nextOption+' which you\'d probably need to run via sh -c\n')

                if 'docker' in nextOption:
                    print('For security reasons, you cannot include "docker" in your command')
                    return False

                self.cmd += nextOption + ' '
            self.cmd = self.cmd.rstrip()
        else:
            print 'You need to specify a command to run'
            return False

        return True

    def printHelp():
        helpstr = """NAME
    socker - Secure runner for Docker containers
SYNOPSIS
    socker run <docker-image> <command>
OPTIONS
    --version
        show the version number and exit
    -h, --help
        show this help message and exit
    -v, --verbose
        run in verbose mode
    images
        List the authorized Docker images (found in socker-images)
    run IMAGE COMMAND
        start a container from IMAGE executing COMMAND as the user
EXAMPLES
    List available images
        $ socker images
    Run a CentOS container and print the system release
        $ socker run centos cat /etc/system-release
    Run the previous command in verbose mode
        $ socker -v run centos cat /etc/system-release
SUPPORT
    Contact hpc-drift@usit.uio.no

        """
        print helpstr    

    def composeDockerCommand():
        """Build and return the string to run the container"""
        dockercmd = 'docker run --name='+ self.containerID +' -d -u '+str(self.user)+':'+str(self.group)

        # TODO: fix list of volumes
        if self.slurm_job_id:
            dockercmd += ' -v $SCRATCH:$SCRATCH -e SCRATCH=$SCRATCH'    
        dockercmd += ' -v /work/:/work/ \
                    -v '+self.PWD+':'+self.PWD+'\
                    -v '+self.home+':'+self.home+'\
                    -w '+self.PWD+' -e HOME='+self.home+\
                    ' '+self.img

        if self.cmd:
            dockercmd += ' '+self.cmd
        
        if verbose:
            print 'container command:\n'+self.cmd+'\n'
            print 'docker command:\n'+dockercmd+'\n'
            print 'executing.....\n'

        return dockercmd

    def startContainer():
        """Start the container as "dockeruser", not as root) """
        p = subprocess.Popen( composeDockerCommand(), \
                              preexec_fn=reincarnate( self.dockeruid, self.dockergid), shell=True, \
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        out,err = p.communicate()
        if p.returncode != 0:
            print '#Error: '+err
            sys.exit(2)
        elif verbose:
            print err
            print 'container ID:\n',out

    def moveContainerCGroup():
        """ Change container from docker to slurm cgroups """
        if self.slurm_job_id:
            # Get the container's PID
            cpid = int( subprocess.Popen("docker inspect -f '{{ .State.Pid }}' "+ self.containerID,\
                                    shell=True, stdout=subprocess.PIPE).stdout.read().strip() )

            # Classify the container process (and all of it's children) to the Slurm's cgroups assigned to the job
            cchildren = subprocess.Popen('pgrep -P'+str(cpid), shell=True, stdout=subprocess.PIPE).stdout.read().split('\n')
            cpids = [cpid] + [int(pid) for pid in cchildren if pid.strip() != '']

            for pid in cpids:
                setSlurmCgroups( user, slurm_job_id, pid )

    def setSlurmCgroups( userID, jobID, containerPID ):
        """ Replace the CGroup of a container with the one defined by a Job """
        cpid = containerPID
        cgroupID = 'slurm/uid_'+str(userID)+'/job_'+str(jobID)+'/step_batch '+str(cpid)

        # Set the container process free from the docker cgroups
        subprocess.Popen('cgclassify -g blkio:/ '+str(cpid), shell=True, stdout=subprocess.PIPE)
        subprocess.Popen('cgclassify -g net_cls:/ '+str(cpid), shell=True, stdout=subprocess.PIPE)
        subprocess.Popen('cgclassify -g devices:/ '+str(cpid), shell=True, stdout=subprocess.PIPE)
        subprocess.Popen('cgclassify -g cpuacct:/ '+str(cpid), shell=True, stdout=subprocess.PIPE)
        subprocess.Popen('cgclassify -g cpu:/ '+str(cpid), shell=True, stdout=subprocess.PIPE)

        # Include the container process in the Slurm cgroups
        out = ''
        out += 'adding '+str(cpid)+' to Slurm\'s memory cgroup: '+\
        subprocess.Popen('cgclassify -g memory:/'+cgroupID, shell=True, stdout=subprocess.PIPE).stdout.read()
        out += '\nadding '+str(cpid)+' to Slurm\'s cpuset cgroup: '+\
        subprocess.Popen('cgclassify -g cpuset:/'+cgroupID, shell=True, stdout=subprocess.PIPE).stdout.read()
        out += '\nadding '+str(cpid)+' to Slurm\'s freezer cgroup: '+\
        subprocess.Popen('cgclassify -g freezer:/'+cgroupID, shell=True, stdout=subprocess.PIPE).stdout.read()
        if self.verbose:
            print out
    
    def waitContainer():
        """Wait for container to finish, asking docker"""
        if self.verbose:
            print 'waiting for the container to exit...\n'
        subprocess.Popen('docker wait '+self.containerID, shell=True, stdout=subprocess.PIPE).stdout.read()

    def captureContainerExitOutput():
        """ After the container exit's, capture it's output"""
        clog = subprocess.Popen( "docker inspect -f '{{.LogPath}}' "+str(self.containerID), \
                                shell=True, stdout=subprocess.PIPE).stdout.read().rstrip()
        with open(clog,'r') as f:
            if self.verbose:
                print 'container output:\n'
            for line in f:
                d = eval(line.replace('\n',''))
                if d['stream'] == 'stderr':
                    sys.stdout.write('#Error: '+d['log'])
                else:
                    sys.stdout.write(d['log'])
        if self.verbose:        
            print '\nremoving the container...\n'
        subprocess.Popen('docker rm '+self.containerID, shell=True, stdout=subprocess.PIPE).stdout.read()

def reincarnate(user_uid, user_gid):
    def result():
        #print 'uid, gid = %d, %d; %s' % (os.getuid(), os.getgid(), 'starting reincarnation')
        os.setgid(user_gid)
        os.setuid(user_uid)
        #print 'uid, gid = %d, %d; %s' % (os.getuid(), os.getgid(), 'ending reincarnation')
    return result


def main(argv):
    # Initialization
    sck = Socker()

    if not sck.initialize():
        print 'Program stopped. Unable to initialize.'
        sys.exit( 2 )

    if not sck.becomeRoot() : sys.exit( 2 )

    if not sck.getDockerVersion() : sys.exit( 2 )
    
    # Checking if help is needed
    if argv[0] in ['-h','--help']:
        sck.printHelp()
        sys.exit( 0 )    

    # Show version
    if argv[0] == '--version':
        print 'Socker version: release '+VERSION
        print 'Docker version: '+dockerv
        sys.exit()

    # Activate verbose 
    if argv[0] in ['-v','--verbose']:
        del argv[0]
        sck.verbose = True

    # List images
    if argv[0] == 'images':
        print '\n'.join(images)
        sys.exit()
        ##This part should be used when you have a secure local docker registry
        # p = subprocess.Popen('docker images', shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        # out,err = p.communicate()
        # if p.returncode == 0:
        #     print out
        #     sys.exit()
        # else:
        #     print err
        #     sys.exit(2)
    # Check if ready to run
    elif argv[0] == 'run':
        try:
            if not sck.safetyCheck( argv ):
                print 'Program stopped. Request support from ' + sck.msgErr_contact
                sys.exit( 2 )
        except:
            print 'The run command should be: socker run <image> <command>'
            sys.exit( 2 )
    # No command, no joy
    else:
        print 'Invalid option'
        print 'type -h or --help for help'
        sys.exit( 2 )
    
    # Start the container (run this command as "dockeruser", not as root)
    startContainer()

    # Change container from docker to slurm cgroups
    moveContainerCGroup()
    
    # Wait container to finish
    waitContainer()
    
    # Capture container's output on exit
    captureContainerExitOutput()

if __name__ == "__main__":
   main( sys.argv[1:] )
