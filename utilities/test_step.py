from pathlib import Path
from typing import List, Optional
import pandas as pd
import numpy as np
import subprocess

import click

def process_comma_separated_values(ctx, param, value):
    if value is not None:
        return value.split(',')
    return []


@click.command()
@click.option(
    "--run_ids_file",
    type=Path,
    default=Path(__file__).parents[0] / "1000_random_run_ids.csv",
    help="A .csv file containing run ids that we can use to test data pipeline steps (one run id per row)",
)
@click.option(
    "--step_id", required=True, callback=process_comma_separated_values, help="Which step(s) to run the test for"
)
@click.option(
    "--test_type",
    type=click.Choice(["run-local", "deploy-test-in-cloud"]),
    required=True,
    help="Whether to test in the cloud or locally",
)
@click.option(
    "--make_path",
    type=Path,
    required=True,
    help="Path to the makefile that contains the commands to run the tests",
)
@click.option(
    "--num_run_ids",
    type=int,
    default=100,
    help="Number of run ids to use for testing",
)
@click.option(
    "--random_seed",
    type=int,
    default=42,
    help="Random seed to use for selecting run ids",
)
@click.option(
    "--extra_run_ids",
    callback=process_comma_separated_values,
    help="Extra run ids to use for testing (set --num_run_ids 0 to only use the run ids passed using this flag)",
)
@click.option(
    "--out_file",
    type=Path,
    help="Path to the output file where stdout and stderr will be written",
)
def main(
    run_ids_file: Path,
    step_id: List[str],
    test_type: str,
    make_path: Path,
    num_run_ids: int,
    random_seed: int,
    extra_run_ids: Optional[List[str]],
    out_file: Optional[Path] = None,
):
    # Read in the run ids
    run_ids_df = pd.read_csv(run_ids_file, header=None)
    run_ids = run_ids_df[0].tolist()

    rng = np.random.default_rng(seed=random_seed)
    selected_run_ids = rng.choice(run_ids, num_run_ids, replace=False).tolist()

    if extra_run_ids is not None:
        selected_run_ids.extend(extra_run_ids)
        selected_run_ids = list(set(selected_run_ids))

    # Build command to run the tests
    command = f"FIXED_RUN_IDS={','.join(selected_run_ids)} ONLY_STEPS={','.join(step_id)} make -C {make_path} {test_type}"
    if out_file is not None and test_type == "run-local":
        command += f" 2>&1 | tee {out_file}"

    print(f"Running the following command: {command}")

    # Start the process and get the PID
    process = subprocess.Popen(command, shell=True)

    # Print the PID
    print(f"Started process with PID: {process.pid}")

    # Wait for the process to complete and capture the return code
    return_code = process.wait()

    # Check the return code and handle errors
    if return_code != 0:
        print(f"Command '{command}' failed with return code {return_code}")
    else:
        print(f"Command '{command}' executed successfully")



if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
