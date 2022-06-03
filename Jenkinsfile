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

def dockerTagAndPush(credentialsID, dockerRegistry, dockerShop, applicationName, imageTag) {
  def image = sh(returnStdout: true, script: "docker inspect --format='{{.Id}}' $applicationName:latest").toString().trim()
  def dockerImage = "$dockerRegistry/$dockerShop/$applicationName"

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
def dockerRegistry = "artifactory.teslamotors.com:2194"
def dockerShop = "battery-pack"
def branchName = getBranchName()

properties([
  pipelineTriggers([githubPush()])
])

podTemplate(label: label,
  containers: [
    containerTemplate(name: 'docker', image: 'artifactory.teslamotors.com:2046/docker:latest', ttyEnabled: true, command: 'cat', resourceLimitCpu: '1' , resourceLimitMemory : '4Gi' ,resourceRequestCpu : '100m' , resourceRequestMemory : '512Mi'),
    containerTemplate(name: 'kubectl', image: 'artifactory.teslamotors.com:2153/atm-baseimages/alpine:kubectl', ttyEnabled: true, command: 'cat',resourceLimitCpu: '1' , resourceLimitMemory : '4Gi' ,resourceRequestCpu : '100m' , resourceRequestMemory : '512Mi'),
  ],
  volumes: [
    hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
      emptyDirVolume(mountPath: '/build', memory: false),
  ]) {

  node(label) {

    git poll: true, branch: env.BRANCH_NAME, credentialsId: 'github-gf1pe-token', url: 'https://github.tesla.com/GF1PE/BMA_output_teams.git'
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
          dockerTagAndPush("gf1pe-docker-registry-creds", dockerRegistry, dockerShop,applicationName, imageTag)
        }

        container('kubectl') {
           withCredentials([file(credentialsId: 'us-sjc37-eng-factory-config', variable: 'KUBECONFIG',)]) {
            sh """
                sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -
            """
           }
        }
      }

      if (branchName ==~ /prod/) {
        println "Deploying to Production\n"

          container('docker') {
          dockerTagAndPush("gf1pe-docker-registry-creds", dockerRegistry, dockerShop,applicationName, imageTag)
        }

        println "Deploying image_tag=${imageTag} \n"
        println "sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -"
        container('kubectl') {
           withCredentials([file(credentialsId: 'us-rno03-prd-factory-config', variable: 'KUBECONFIG',)]) {
          sh """
              sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/bmaoutput.yaml | kubectl apply -f -
          """
          }
        }
      }
    }
  }
}
