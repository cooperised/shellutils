# Job pooling for bash shell scripts
# This script provides a job pooling functionality where you can keep up to n
# processes/functions running in parallel so that you don't saturate a system
# with concurrent processes.
#
# Got inspiration from http://stackoverflow.com/questions/6441509/how-to-write-a-process-pool-bash-shell
#
# Copyright (c) 2012 Vince Tse
# with changes by Geoff Clements (c) 2014
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# end-of-jobs marker
job_pool_end_of_jobs="JOBPOOL_END_OF_JOBS"

# sequential tasks are given this mode
job_pool_sequential_tag="seq"

# job queue used to send jobs to the workers
job_pool_job_queue=/tmp/job_pool_job_queue_$$

# lock file and mode file for job locking
job_pool_mode_file=/tmp/job_pool_mode_file_$$

# where to run results to
job_pool_result_log=/tmp/job_pool_result_log_$$

# toggle command echoing
job_pool_echo_command=0

# number of parallel jobs allowed.  also used to determine if job_pool_init
# has been called when jobs are queued.
job_pool_pool_size=-1

# \brief variable to check for number of non-zero exits
job_pool_nerrors=0

################################################################################
# private functions
################################################################################

# \brief debug output
function _job_pool_echo()
{
    if [[ "${job_pool_echo_command}" == "1" ]]; then
        echo $@
    fi
}

# \brief cleans up
function _job_pool_cleanup()
{
    local pool_name=$1
    local job_queue=/tmp/job_pool_job_queue_${pool_name}
    local result_log=/tmp/job_pool_result_log_${pool_name}
    local mode_file=/tmp/job_pool_mode_file_${pool_name}
    rm -f ${job_queue} ${result_log} ${mode_file}
}

# \brief signal handler
function _job_pool_exit_handler()
{
    _job_pool_stop_workers
    _job_pool_cleanup
}

# \brief print the exit codes for each command
# \param[in] result_log  the file where the exit codes are written to
function _job_pool_print_result_log()
{
    local pool_name=$1
    local result_log=/tmp/job_pool_result_log_${pool_name}

    job_pool_nerrors=$(grep ^ERROR "${result_log}" | wc -l)
    cat "${result_log}" | sed -e 's/^ERROR//'
}

# \brief the worker function that is called when we fork off worker processes
# \param[in] id  the worker ID
# \param[in] job_queue  the fifo to read jobs from
# \param[in] result_log  the temporary log file to write exit codes to
# \param[in] mode_file  the lockfile and mode file for barriers
function _job_pool_worker()
{
    local pool_name=$1
    local id=$2
    local job_queue=/tmp/job_pool_job_queue_${pool_name}
    local result_log=/tmp/job_pool_result_log_${pool_name}
    local mode_file=/tmp/job_pool_mode_file_${pool_name}
    local mode=
    local cmd=
    local args=

    exec 7<> ${job_queue}
    exec 9<> ${mode_file}
    while [[ "${mode}" != "${job_pool_end_of_jobs}" && -e "${job_queue}" ]]; do
        # workers block on the exclusive lock to read the job queue
        flock --exclusive 7
        flock --shared 9
        IFS=$'\v'
        read mode cmd args <${job_queue}
        if ! grep -Fxq $mode ${mode_file}; then
          flock --exclusive 9
          echo "$mode" > ${mode_file};
          flock --shared 9
        fi
        set -- ${args}
        unset IFS
        if [[ "${job_pool_sequential_tag}" != "${mode}" ]]; then
          flock --unlock 7
        fi
        # the worker should exit if it sees the end-of-job marker or run the
        # job otherwise and save its exit code to the result log.
        if [[ "${mode}" == "${job_pool_end_of_jobs}" ]]; then
            # write it one more time for the next sibling so that everyone
            # will know we are exiting.
            echo "${mode}" >&7
        else
            _job_pool_echo "### _job_pool_worker-${id}: ${cmd}"
            # run the job
            ( eval ${cmd} "$@" ; )
            # now check the exit code and prepend "ERROR" to the result log entry
            # which we will use to count errors and then strip out later.
            local result=$?
            local status=
            if [[ "${result}" != "0" ]]; then
                status=ERROR
            fi
            # now write the error to the log, making sure multiple processes
            # don't trample over each other.
            exec 8<> ${result_log}
            flock --exclusive 8
            _job_pool_echo "${status}job_pool: exited ${result}: ${cmd} $@" >> ${result_log}
            flock --unlock 8
            exec 8>&-
            _job_pool_echo "### _job_pool_worker-${id}: exited ${result}: ${cmd} $@"
        fi
        flock --unlock 9
        if [[ "${job_pool_sequential_tag}" == "${mode}" ]]; then
          flock --unlock 7
        fi
    done
    exec 7>&-
    exec 9>&-
}

# \brief sends message to worker processes to stop
function _job_pool_stop_workers()
{
    local pool_name=$1
    local job_queue=/tmp/job_pool_job_queue_${pool_name}
    
    # send message to workers to exit, and wait for them to stop before
    # doing cleanup.
    echo ${job_pool_end_of_jobs} >> ${job_queue}
    wait
    # TODO this isn't good enough - what if there are other forked jobs?
}

# \brief fork off the workers
# \param[in] job_queue  the fifo used to send jobs to the workers
# \param[in] result_log  the temporary log file to write exit codes to
# \param[in] mode_file  the lockfile and mode file for barriers
function _job_pool_start_workers()
{
    local pool_name=$1
    local pool_size=$2

    for ((i=0; i<${pool_size}; i++)); do
        _job_pool_worker ${pool_name} ${i} &
    done
}

################################################################################
# public functions
################################################################################

# \brief initializes the job pool
# \param[in] pool_name  name of the job pool
# \param[in] pool_size  number of parallel jobs allowed
# \param[in] echo_command  1 to turn on echo, 0 to turn off
function job_pool_init()
{
    local pool_name=$1
    local pool_size=$2
    local echo_command=$3

    local job_queue=/tmp/job_pool_job_queue_${pool_name}
    local result_log=/tmp/job_pool_result_log_${pool_name}
    local mode_file=/tmp/job_pool_mode_file_${pool_name}

    # set the global attibutes
    job_pool_echo_command=${echo_command:=0}

    # create the fifo job queue and create the exit code log and mode file
    rm -rf ${job_queue} ${result_log} ${mode_file}
    mkfifo -m 600 ${job_queue}
    touch ${result_log}
    touch ${mode_file}

    # fork off the workers
    _job_pool_start_workers ${pool_name} ${pool_size}
}

# \brief waits for all queued up jobs to complete and shuts down the job pool
function job_pool_shutdown()
{
    local pool_name=$1

    _job_pool_stop_workers ${pool_name}
    _job_pool_print_result_log ${pool_name}
#    _job_pool_cleanup ${pool_name}
}

# \brief run a job in the job pool
function job_pool_run()
{
#    if [[ "${job_pool_pool_size}" == "-1" ]]; then
#        job_pool_init
#    fi
    local pool_name=$1
    local job_queue=/tmp/job_pool_job_queue_${pool_name}
    
    shift
    
    printf "%s\v" "$@" >> ${job_queue}
    echo >> ${job_queue}
}

# \brief waits for all queued up jobs to complete before starting new jobs
# This function actually fakes a wait by telling the workers to exit
# when done with the jobs and then restarting them.
function job_pool_wait()
{
    local pool_name=$1
    local sem=$(mktemp -u)
    mkfifo -m 600 ${sem}
    job_pool_run ${pool_name} ${job_pool_sequential_tag} "echo > ${sem}"
    cat ${sem} > /dev/null
    rm ${sem}
}
#########################################
# End of Job Pool
#########################################

cmd=$1
shift
case $cmd in
create)
  job_pool_init "$@"
  ;;
run)
  job_pool_run "$@"
  ;;
shutdown)
  job_pool_shutdown "$@"
  ;;
"wait")
  job_pool_wait "$@"
  ;;
esac

