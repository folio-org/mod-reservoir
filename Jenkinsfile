buildMvn {
  publishModDescriptor = true
  mvnDeploy = true
  buildNode =  'jenkins-agent-java17'

  doApiLint = true
  doApiDoc = true
  apiTypes = 'OAS'
  apiDirectories = 'server/src/main/resources/openapi'
  apiExcludes = 'headers parameters'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = true
      healthChkCmd = 'wget --no-verbose --tries=1 --spider http://localhost:8081/admin/health || exit 1'
    }
  }
}

