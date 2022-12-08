import os
from dagster import job, op, Failure, Nothing, graph, In
from dagster_shell.utils import execute
from pathlib import Path


@op
def run_shell_command(context, shell_command: str, env=None):
    """ A templated solid for running cli commands.

        Args: 
            shell_command (str): The shell command to be run
            env (Dict[str,str], optional): Environment variables to pass to run

        Returns:
            str: Combined stdout/stderr output of running the shell
    """

    home = str(Path.home())
    if env is None:
        env = {}
    elif not isinstance(env,dict):
        raise Failure(description="Shell command execution failed with non-confirming env dict: {env}")
    
# meltano will need "PATH" env var from wherever its run

    env_merged = {**os.environ.copy(), **env} if env else os.environ.copy()

    output, return_code = execute(
        shell_command=shell_command,
        log=context.log,
        output_logging="STREAM",
        env=env_merged,
        cwd= home + '/Repos/ownbackup/meltano-example/'
        ) 
    
    if return_code:
        raise Failure(description="Shell command execution failed with output: {output}".format(output=output))
    return output


@op
def activate_virtual_env() -> str:
    """activate the correct virtual env"""
    return "source ~/Repos/venv/bin/activate"

@op
def meltano_opoid_elt_cmd(ins={"start":In(Nothing)}) -> str:
    """ Generates a meltano elt command for the opoid_usage api pipeline"""
    return "meltano --environment dev run tap-rest-api-msdk target-jsonl"

@op(ins={"start": In(Nothing)})
def s3_sync_tap_output() -> str:
    """ Generates a awscli command that syncs the data/opoid_usage folder with the s3 bucket"""
    return "aws s3 sync ~/Repos/meltano-example/data/opoid_usage s3://meltano-elt-output"

@job
def opoid_usage_pipeline():
    run_shell_command(meltano_opoid_elt_cmd())
    run_shell_command(s3_sync_tap_output())
