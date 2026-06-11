pipeline {
    agent any

    parameters {
        choice(
            name: 'LOAD_TOOL',
            choices: ['SQOOP', 'SPARK'],
            description: 'Choose Sqoop or Spark'
        )

        choice(
            name: 'LOAD_TYPE',
            choices: ['FULL', 'INCREMENTAL'],
            description: 'Choose full or incremental load'
        )

        choice(
            name: 'LOAD_SCOPE',
            choices: ['ALL', 'DIMENSIONS_ONLY', 'FACT_ONLY', 'SINGLE_TABLE'],
            description: 'Used mainly for Sqoop table loading'
        )

        string(
            name: 'TABLE_NAME',
            defaultValue: '',
            description: 'Required only when LOAD_SCOPE = SINGLE_TABLE. Example: dim_networks'
        )
    }

    environment {
        HDFS_RAW_BASE  = '/tmp/tfl_project_hadoop'
        HDFS_GOLD_BASE = '/tmp/tfl_project_hadoop/gold'

        SQOOP_FULL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/full_load/raw_sqoop_full_load.sh'
        SQOOP_INCREMENTAL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load/raw_incremental_load.sh'

        SPARK_FULL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/full_load/spark/tfl_spark_analysis.py'
        SPARK_INCREMENTAL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load/spark/incremental.py'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Validate Parameters') {
            steps {
                script {
                    if (params.LOAD_SCOPE == 'SINGLE_TABLE' && params.TABLE_NAME.trim() == '') {
                        error "TABLE_NAME is required when LOAD_SCOPE is SINGLE_TABLE"
                    }

                    echo "LOAD_TOOL      = ${params.LOAD_TOOL}"
                    echo "LOAD_TYPE      = ${params.LOAD_TYPE}"
                    echo "LOAD_SCOPE     = ${params.LOAD_SCOPE}"
                    echo "TABLE_NAME     = ${params.TABLE_NAME}"
                    echo "HDFS_RAW_BASE  = ${env.HDFS_RAW_BASE}"
                    echo "HDFS_GOLD_BASE = ${env.HDFS_GOLD_BASE}"
                }
            }
        }

        stage('Select Sqoop Tables') {
            when {
                expression {
                    return params.LOAD_TOOL == 'SQOOP'
                }
            }

            steps {
                script {
                    def dimensions = [
                        'dim_date',
                        'dim_lines',
                        'dim_networks',
                        'dim_stations'
                    ]

                    def factTables = [
                        'fact_passenger_entry_exit',
                        'fact_station_lines'
                    ]

                    if (params.LOAD_SCOPE == 'ALL') {
                        env.TABLE_LIST = (dimensions + factTables).join(',')
                    } else if (params.LOAD_SCOPE == 'DIMENSIONS_ONLY') {
                        env.TABLE_LIST = dimensions.join(',')
                    } else if (params.LOAD_SCOPE == 'FACT_ONLY') {
                        env.TABLE_LIST = factTables.join(',')
                    } else if (params.LOAD_SCOPE == 'SINGLE_TABLE') {
                        env.TABLE_LIST = params.TABLE_NAME.trim()
                    }

                    echo "Sqoop tables selected: ${env.TABLE_LIST}"
                }
            }
        }

        stage('Run Sqoop Load') {
            when {
                expression {
                    return params.LOAD_TOOL == 'SQOOP'
                }
            }

            steps {
                script {
                    def tables = env.TABLE_LIST.split(',')

                    for (table in tables) {
                        table = table.trim()

                        def targetSuffix = ''
                        if (params.LOAD_TYPE == 'FULL') {
                            targetSuffix = '_full_load'
                        } else {
                            targetSuffix = '_inc_load'
                        }

                        def hdfsTargetPath = "${env.HDFS_RAW_BASE}/${table}${targetSuffix}"

                        echo "=================================================="
                        echo "Running Sqoop ${params.LOAD_TYPE} load"
                        echo "Table           : ${table}"
                        echo "HDFS target path: ${hdfsTargetPath}"
                        echo "=================================================="

                        if (params.LOAD_TYPE == 'FULL') {
                            sh """
                                chmod +x ${SQOOP_FULL_SCRIPT}
                                ${SQOOP_FULL_SCRIPT} ${table} ${hdfsTargetPath}
                            """
                        }

                        if (params.LOAD_TYPE == 'INCREMENTAL') {
                            sh """
                                chmod +x ${SQOOP_INCREMENTAL_SCRIPT}
                                ${SQOOP_INCREMENTAL_SCRIPT} ${table} ${hdfsTargetPath}
                            """
                        }
                    }
                }
            }
        }

        stage('Run Spark Full Flow') {
            when {
                expression {
                    return params.LOAD_TOOL == 'SPARK' && params.LOAD_TYPE == 'FULL'
                }
            }

            steps {
                sh """
                    echo "Running Spark full gold flow"
                    echo "Gold destination: ${HDFS_GOLD_BASE}"

                    spark-submit ${SPARK_FULL_SCRIPT}
                """
            }
        }

        stage('Run Spark Incremental Flow') {
            when {
                expression {
                    return params.LOAD_TOOL == 'SPARK' && params.LOAD_TYPE == 'INCREMENTAL'
                }
            }

            steps {
                sh """
                    echo "Running Spark incremental gold flow"
                    echo "Gold destination: ${HDFS_GOLD_BASE}"

                    spark-submit ${SPARK_INCREMENTAL_SCRIPT}
                """
            }
        }

        stage('Validate HDFS Output') {
            steps {
                script {
                    if (params.LOAD_TOOL == 'SQOOP') {
                        sh """
                            echo "Validating Sqoop raw output"
                            hdfs dfs -ls ${HDFS_RAW_BASE} || true
                        """
                    }

                    if (params.LOAD_TOOL == 'SPARK') {
                        sh """
                            echo "Validating Spark gold output"
                            hdfs dfs -ls ${HDFS_GOLD_BASE} || true
                        """
                    }
                }
            }
        }
    }

    post {
        success {
            echo "Pipeline completed successfully."
        }

        failure {
            echo "Pipeline failed. Check Jenkins console logs."
        }
    }
}