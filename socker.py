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
        self.images = None
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

        if not [g.gr_name for g in grp.getgrall() if self.dockerusr in g.gr_mem] == [ self.dockergrp ]:
            print 'The user "'+ self.dockerusr +'" must be a member of ONLY the "'+ self.dockergrp + '" group'
            return False
        
        # Get the current user information
        self.user = os.getuid()
        self.group = os.getgid()
        self.containerID = str( uuid.uuid4() )
        
        try:
            self.slurm_job_id = os.environ['SLURM_JOB_ID']
            print 'Slurm job id', self.slurm_job_id
        except KeyError as e:
            #print e,slurm_job_id
            pass
        
        return True

    def buildVolumesArgs( self ):
        """ return string with list of volumes to build the docker call """
        hstVols = []
        cntVols = []
        strDockerCmd = ''

        # DEFINE AS MANY AS NEEDED - Cluster site specific!

        # Share current working directory
        PWD = os.getcwd()
        hstVols.append( PWD )
        cntVols.append( PWD )

        # Share user's $home
        home = pwd.getpwuid( self.user ).pw_dir
        hstVols.append( home )
        cntVols.append( home )

        # Share work folder
        work = '/work/'
        hstVols.append( work )
        cntVols.append( work )

        # Share scratch folder
        if self.slurm_job_id:
            scratch = '$SCRATCH'
            hstVols.append( scratch )
            cntVols.append( scratch )

        for hostVolume, containerVolume in zip( hstVols, cntVols ):
            strDockerCmd += ' -v '+ hostVolume+':'+containerVolume

        # Define  -w, --workdir string  Working directory inside the container
        strDockerCmd += ' -w '+ PWD

        return strDockerCmd

    def buildEnvArgs( self ):
        """ return string with list of environmental variables to build the docker call """
        envVar = []
        envVal = []
        strDockerCmd = ''

        # DEFINE AS MANY AS NEEDED - Cluster site specific!

        # Define home folder
        envVar.append( 'HOME' )
        envVal.append( pwd.getpwuid( self.user ).pw_dir )

        # Define scratch var
        if self.slurm_job_id:
            envVar.append( 'SCRATCH' )
            envVal.append( '$SCRATCH' )

        for envVarName, envValue in zip( envVar, envVal ):
            strDockerCmd += ' -v '+ envVarName+'='+envValue

        return strDockerCmd

    def becomeRoot( self ):
        """Change the user and group running the process to root:root"""
        try:
            # Set the user to root
            os.setuid(0)
            os.setgid(0)
        except:
            print 'Unable to become root.'
            return False

        return True

    def getDockerVersion( self ):
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

    def loadImages( self ):
        """ Get the list of authorized images """
        try:
            self.images = filter(None,[line.strip() for line in open( self.socker_images_file,'r')])
            if len(self.images) == 0:
                raise Exception()
        except:
            print 'No authorized images to run. Socker cannot be used at the moment.\nContact ' + self.msgErr_contact 
            if self.verbose:
                e = sys.exc_info()[0]
                print 'Error: '+str( e )
            return False
        else:
            return True
        ##This part should be used when you have a secure local docker registry
        # p = subprocess.Popen('docker images', shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        # out,err = p.communicate()
        # if p.returncode == 0:
        #     print out
        #     sys.exit()
        # else:
        #     print err
        #     sys.exit(2)

    def safetyChecks( self, cmdOptions ):
        """Second phase initialization, checking values to ensure it is safe to run while building cmd string"""

        if len(cmdOptions) < 2:
            print 'You need to specify an image to run'
            return False

        self.img = cmdOptions[1]
        if not self.img in self.images:
            if not 'ALL' in self.images: 
              print '"'+ self.img +'" is not an authorized image for this system. Please send a request to ' + self.msgErr_contact
            return False

        # Check commands==remainingOptions to be run inside the container
        if len(cmdOptions) >2:
            self.cmd = ''

            for nextOption in cmdOptions[2:] :
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

    def printHelp( self ):
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

    def composeDockerCommand( self ):
        """Build and return the string to run the container"""
        dockercmd = 'docker run --name='+ self.containerID +' -d -u '+str(self.user)+':'+str(self.group)
        dockercmd += self.buildVolumesArgs()
        dockercmd += self.buildEnvArgs()
        dockercmd += ' '+self.img

        if self.cmd:
            dockercmd += ' '+self.cmd
        
        if self.verbose:
            print 'container command:\n'+self.cmd+'\n'
            print 'docker command:\n'+dockercmd+'\n'
            print 'executing.....\n'

        return dockercmd

    def startContainer( self ):
        """Start the container as "dockeruser", not as root) """
        p = subprocess.Popen( self.composeDockerCommand(), \
                              preexec_fn=reincarnate( self.dockeruid, self.dockergid), shell=True, \
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        out,err = p.communicate()
        if p.returncode != 0:
            print '#Error: '+err
            sys.exit(2)
        elif self.verbose:
            print err
            print 'container ID:\n',out

    def moveContainerCGroup( self ):
        """ Change container from docker to slurm cgroups """
        if self.slurm_job_id:
            # Get the container's PID
            cpid = int( subprocess.Popen("docker inspect -f '{{ .State.Pid }}' "+ self.containerID,\
                                    shell=True, stdout=subprocess.PIPE).stdout.read().strip() )

            # Classify the container process (and all of it's children) to the Slurm's cgroups assigned to the job
            cchildren = subprocess.Popen('pgrep -P'+str(cpid), shell=True, stdout=subprocess.PIPE).stdout.read().split('\n')
            cpids = [cpid] + [int(pid) for pid in cchildren if pid.strip() != '']

            for pid in cpids:
                self.setSlurmCgroups( self, self.user, self.slurm_job_id, pid )

    def setSlurmCgroups( self, containerPID ):
        """ Replace the CGroup of a container with the one defined by a Job """
        cpid = containerPID
        cgroupID = 'slurm/uid_'+str(self.user)+'/job_'+str(self.slurm_job_id)+'/step_batch '+str(cpid)

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
    
    def waitContainer( self ):
        """Wait for container to finish, asking docker"""
        if self.verbose:
            print 'waiting for the container to exit...\n'
        subprocess.Popen('docker wait '+self.containerID, shell=True, stdout=subprocess.PIPE).stdout.read()

    def captureContainerExitOutput( self ):
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
    """ socker main algorithm """

    # Initialization
    sck = Socker()

    if not sck.initialize():
        print 'Program stopped. Unable to initialize.'
        sys.exit( 2 )

    # Checking empty args
    if len(argv) == 0:
        sck.printHelp()
        sys.exit( 0 )

    # Checking if help is needed
    if argv[0] in ['-h','--help']:
        sck.printHelp()
        sys.exit( 0 ) 

    if not sck.becomeRoot() : sys.exit( 2 )

    if not sck.getDockerVersion() : sys.exit( 2 )
    
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
        if not sck.loadImages(): 
            sys.exit( 2 )
        print '\n'.join( sck.images )
        sys.exit()

    # Check if ready to run
    elif argv[0] == 'run':
        if not sck.safetyChecks( argv ):
            print 'Program stopped. Request support from ' + sck.msgErr_contact
            sys.exit( 2 )
    # No command, no joy
    else:
        print 'Invalid option'
        print 'type -h or --help for help'
        sys.exit( 2 )
    
    # Start the container (run this command as "dockeruser", not as root)
    sck.startContainer()

    # Change container from docker to slurm cgroups
    sck.moveContainerCGroup()
    
    # Wait container to finish
    sck.waitContainer()
    
    # Capture container's output on exit
    sck.captureContainerExitOutput()

if __name__ == "__main__":
    main( sys.argv[1:] )
