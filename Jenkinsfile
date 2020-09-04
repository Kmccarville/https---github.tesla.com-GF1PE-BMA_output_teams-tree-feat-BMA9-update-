// Helper functions
def getBranchName() {
  return env.BRANCH_NAME == "master" ? "prod" :
         env.BRANCH_NAME == "dev" ? "dev" : "NONE"
}

def getImageTag(){
  def commit = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
  def branch = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
  def git_rev = sh(script: "git rev-parse HEAD", returnStdout: true).trim().take(7)
  def image_tag = "${git_rev}-${env.BUILD_NUMBER}"
  return image_tag
}

def dockerLogin(credentialsID, dockerRegistry) {
  withCredentials([usernamePassword(credentialsId: credentialsID, usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
      sh "docker login $dockerRegistry -u='$USERNAME' -p='$PASSWORD'"
  }
}

def dockerTagAndPush(credentialsID, dockerRegistry, applicationName, imageTag) {
  def image = sh(returnStdout: true, script: "docker inspect --format='{{.Id}}' $applicationName:latest").toString().trim()
  def dockerImage = "$dockerRegistry/tesla/m3bppe/$applicationName"

  dockerLogin(credentialsID, dockerRegistry)

  sh "docker tag $image $dockerImage:$imageTag"
  sh "docker push $dockerImage:$imageTag"
}

// Build variables
import static java.util.UUID.randomUUID
def uuid = randomUUID() as String
def label = "build-" + uuid.take(8)

// Application variables
def applicationName = "bmaoutput"
def dockerRegistry = "artifactory.teslamotors.com:2002"
def branchName = getBranchName()

properties([
  pipelineTriggers([pollSCM('H/5 * * * *')])
])

podTemplate(label: label,
  containers: [
    containerTemplate(name: 'docker', image: 'docker:latest', ttyEnabled: true, command: 'cat'),
    containerTemplate(name: 'kubectl', image: 'lachlanevenson/k8s-kubectl', ttyEnabled: true, command: 'cat'),
  ],
  volumes: [
    hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
      emptyDirVolume(mountPath: '/build', memory: false),
  ]) {

  node(label) {

    git poll: true, branch: env.BRANCH_NAME, credentialsId: 'git-creds', url: 'ssh://git@stash.teslamotors.com:7999/gf1pe/bma_output_teams.git'
    def imageTag = getImageTag()

    stage('Build') {
      container('docker') {
        sh "docker build --network host -t $applicationName ."
      }
    }

    stage("Deploy") {
      if (branchName ==~ /dev/) {
        println "Deploying to Development\n"

        container('docker') {
          dockerTagAndPush("docker-registry-creds", dockerRegistry, applicationName, imageTag)
        }

        container('kubectl') {
          sh """
              sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -
          """
        }
      }

      if (branchName ==~ /prod/) {
        println "Deploying to Production\n"

        container('docker') {
          dockerTagAndPush("docker-registry-creds", dockerRegistry, applicationName, imageTag)
        }
        println "Deploying image_tag=${imageTag} \n"
        println "sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -"
        container('kubectl') {
          withCredentials([file(credentialsId: 'k8s-prod-RNO-cred', variable: 'KUBECONFIG',)]) {
          sh """
              sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -
          """
          }
        }
      }
    }
  }
}