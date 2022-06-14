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

// Application variables
def applicationName = "bmaoutput" //replace with your application name
def dockerRegistry = "artifactory.teslamotors.com:2194" //gf1pe-docker-local artifactory repo
def dockerShop = "battery-module" //replace with your shop name | nested folder under gf1pe-docker-local
def branchName = getBranchName()

properties([
  pipelineTriggers([githubPush()]) //github webhook trigger
])

node('build'){
    stage('CheckoutSCM'){
        println "inside node\n"
        //checkout scm
        git poll: true, branch: env.BRANCH_NAME, credentialsId: 'github-gf1pe-token', url: 'https://github.tesla.com/GF1PE/BMA_output_teams.git'
        println "after github poll\n"
    }
    stage('Build Docker'){
        docker.withRegistry('https://artifactory.teslamotors.com:2194', 'gf1pe-docker-registry-creds') {
            def imageTag = getImageTag()
            def customImage = docker.build("$dockerShop/$applicationName:$imageTag")
            customImage.push()
        }
    }
    stage('Deploy k8s'){
        if (branchName ==~ /dev/) {
        println "Deploying to Development\n"
            withKubeConfig([
                credentialsId: 'us-sjc37-eng-factory-config',
                namespace: 'gf1-pe'
            ]) {
                sh """
                    sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/${applicationName}.yaml | kubectl apply -f -
                """
            }
        }

        if (branchName ==~ /prod/) {
        println "Deploying to Production\n"
        println "Deploying image_tag=${imageTag} \n"

            withKubeConfig([
                credentialsId: 'us-sjc37-eng-factory-config',
                namespace: 'gf1-pe'
            ]) {
                sh """
                    sed 's/\$IMG_TAG/${imageTag}/g' k8s/${branchName}/${applicationName}.yaml | kubectl apply -f -
                """
            }
        }
    }
}