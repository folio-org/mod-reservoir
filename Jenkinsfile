buildMvn {
  publishModDescriptor = true
  mvnDeploy = true
  buildNode =  'jenkins-agent-java11'

  doApiLint = true
  doApiDoc = true
  apiTypes = 'OAS'
  apiDirectories = 'server/src/main/resources/openapi'
  apiExcludes = 'headers parameters'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = true
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/admin/health || exit 1'
    }
  }
}

