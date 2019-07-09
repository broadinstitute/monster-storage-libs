pipeline {
    agent any

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('sbt')}:$PATH"
    }
    stages {
        stage('Check formatting') {
            steps {
                sh 'sbt scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt Compile/compile Test/compile IntegrationTest/compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt test'
            }
        }
        stage('Integration Test') {
            environment {
                // Some wiring is broken between the custom-tools plugin and
                // the pipeline plugin which prevents these vars from being
                // injected when pulling in the custom 'vault' tool.
                VAULT_ADDR = 'https://clotho.broadinstitute.org:8200'
                VAULT_TOKEN_PATH = '/etc/vault-token-monster'
            }
            steps {
                sh 'sbt IntegrationTest/test'
            }
        }
    }
    post {
        always {
            junit '**/target/test-reports/*'
        }
        cleanup {
            cleanWs()
        }
    }
}
