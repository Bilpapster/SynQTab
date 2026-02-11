import json
import os
import subprocess
import time
from pathlib import Path
import yaml
from typing import Dict, Set
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv
from synqtab.utils.logging_utils import get_logger
from synqtab.utils.discord_utils import notify_script_complete, notify_script_failed, notify_batch_summary

logger = get_logger(__name__)

class KernelStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class KernelJob:
    script_path: str
    kernel_slug: str
    status: KernelStatus
    retry_count: int = 0


load_dotenv()
def execute_single_script(
        script_path: str,
        username: str = str,
        title: str = "Kaggle Execution",
        enable_gpu: bool = False,
        enable_internet: bool = True,
        is_private: bool = True,
        accelerator: str = "NvidiaTeslaT4"
) -> KernelStatus:
    """
    Execute a Python script on Kaggle using the Kaggle Kernels API.

    Args:
        script_path: Path to the .py file to execute
        username: Your Kaggle username
        title: Title for the Kaggle kernel
        enable_gpu: Whether to enable GPU
        enable_internet: Whether to enable internet
        is_private: Whether the kernel should be private
        accelerator: Accelerator ID (e.g., "NvidiaTeslaP100", "NvidiaTeslaT4", "TpuV6E8"), defaults to "NvidiaTeslaT4"
    """
    script_path = Path(script_path)

    if not script_path.exists() or script_path.suffix != '.ipynb':
        raise ValueError("Invalid Python file path")

    # Create kernel metadata
    kernel_slug = title.lower().replace(' ', '-')

    metadata = {
        "id": f"{username}/{kernel_slug}",
        "title": title,
        "code_file": script_path.name,
        "language": "python",
        "kernel_type": "notebook",
        "is_private": str(is_private),
        "enable_gpu": str(enable_gpu),
        "enable_tpu": "false",
        "enable_internet": str(enable_internet),
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "model_sources": []
    }

    # Create temp directory for kernel files
    temp_dir = Path("temp_kaggle_kernel")
    temp_dir.mkdir(exist_ok=True)

    # Write metadata
    metadata_path = temp_dir / "kernel-metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    # Copy original notebook to temp directory
    temp_notebook_path = temp_dir / script_path.name
    import shutil
    shutil.copy(script_path, temp_notebook_path)

    # Fix notebook metadata to ensure Kaggle compatibility
    fix_notebook_metadata(temp_notebook_path)

    # 6. Push to Kaggle
    push_command = ["kaggle", "kernels", "push", "-p", str(temp_dir)]
    
    # Add accelerator if specified
    if accelerator:
        push_command.extend(["--accelerator", accelerator])
    
    result = subprocess.run(
        push_command,
        capture_output=True,
        text=True
    )

    # Cleanup
    shutil.rmtree(temp_dir)

    if "Maximum weekly GPU quota" in result.stdout:
        logger.error(f"GPU Quota has been reached for {username}")
        return KernelStatus.FAILED
    elif "Notebook not found" in result.stdout:
        logger.error(f"Notebook not present in Kaggle UI. Please upload a first version of your notebook using kaggle.com")
        return KernelStatus.FAILED
    
    logger.info(result.stdout)
    return KernelStatus.COMPLETE

def fix_notebook_metadata(file_path):
    import nbformat
    # Load the notebook file
    with open(file_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    # Define the kernel specification Kaggle expects
    kernelspec = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3"
    }

    # Overwrite the metadata block
    nb.metadata['kernelspec'] = kernelspec
    
    # Optional: Add language info for completeness
    nb.metadata['language_info'] = {
        "name": "python",
        "version": "3.10.12"
    }

    # Save the updated notebook
    with open(file_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)


def get_kaggle_kernel_status(kernel_slug: str, username: str) -> dict:
    """
    Get the current status of a Kaggle kernel.

    Args:
        kernel_slug: The kernel slug (derived from title)
        username: The Kaggle username for this kernel

    Returns:
        dict: Dictionary containing status information
    """
    kernel_ref = f"{username}/{kernel_slug}"

    result = subprocess.run(
        ["kaggle", "kernels", "status", kernel_ref],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to get kernel status: {result.stderr}")

    # Parse the output
    output = result.stdout.strip()

    # Determine status from output
    status_info = {
        "kernel_ref": kernel_ref,
        "raw_output": output,
        "is_complete": "complete" in output.lower(),
        "has_error": "error" in output.lower(),
        "is_running": "running" in output.lower() or "queued" in output.lower()
    }

    return status_info


# MULTI-PROFILE SUPPORT
@dataclass
class ProfileJob:
    """Represents a job for a specific Kaggle profile."""
    profile_name: str
    script_path: str
    kernel_slug: str
    status: KernelStatus
    retry_count: int = 0


def run_kaggle_scripts_multi_profile(
        yaml_path: str,
        max_retries: int = 3,
        check_interval: int = 30
):
    """
    Execute multiple Kaggle scripts across different profiles based on YAML configuration.

    The YAML file should have the following structure:

    profiles:
      - name: profile1
        credential_name: bilpapster
        max_concurrent: 5
        scripts:
          - path/to/script1.py
          - path/to/script2.py
      - name: profile2
        credential_name: georgeatmatzidis
        max_concurrent: 3
        scripts:
          - path/to/script3.py

    common_settings:
      title_prefix: experiment
      enable_gpu: true
      enable_internet: true
      is_private: true

    Args:
        yaml_path: Path to YAML file with multi-profile Kaggle settings
        max_retries: Maximum retry attempts for failed scripts
        check_interval: Seconds between status checks

    Returns:
        Dict with completed, failed scripts, and job details per profile
    """
    # Load YAML configuration
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)

    profiles_config = config.get('profiles', [])
    common_settings = config.get('common_settings', {})

    if not profiles_config:
        raise ValueError("No profiles defined in YAML configuration")

    # Extract common settings
    title_prefix = common_settings.get('title_prefix', 'Script')
    enable_gpu = common_settings.get('enable_gpu', True)
    enable_internet = common_settings.get('enable_internet', True)
    is_private = common_settings.get('is_private', True)
    accelerator = common_settings.get('accelerator', 'NvidiaTeslaT4')

    # Initialize jobs per profile
    all_jobs: Dict[str, ProfileJob] = {}
    profile_info: Dict[str, Dict] = {}

    for profile_config in profiles_config:
        profile_name = profile_config['name']
        credential_name = profile_config['credential_name']
        max_concurrent = profile_config.get('max_concurrent', 5)
        scripts = profile_config.get('scripts', [])

        profile_info[profile_name] = {
            'credential_name': credential_name,
            'max_concurrent': max_concurrent,
            'running': set(),
            'completed': set(),
            'failed': set(),
            'username': credential_name  # Credential name must match kaggle username
        }

        # Create jobs for this profile
        for idx, script_path in enumerate(scripts):
            kernel_slug = f"{title_prefix}-{profile_name}-{idx}".lower().replace(' ', '-')
            job_id = f"{profile_name}::{script_path}"

            all_jobs[job_id] = ProfileJob(
                profile_name=profile_name,
                script_path=script_path,
                kernel_slug=kernel_slug,
                status=KernelStatus.PENDING
            )

    # Calculate total jobs
    total_jobs = len(all_jobs)
    total_completed = 0
    total_failed = 0

    def mark_failed(job_id: str) -> None:
        """Mark a job as failed exactly once and update counters."""
        nonlocal total_failed
        job = all_jobs[job_id]
        profile = profile_info[job.profile_name]

        if job_id in profile['failed']:
            return

        profile['failed'].add(job_id)
        job.status = KernelStatus.FAILED
        total_failed += 1

    def submit_job(job_id: str) -> bool:
        """Submit a job to Kaggle for a specific profile. Returns True if successful."""
        job = all_jobs[job_id]
        profile = profile_info[job.profile_name]

        try:
            # Set credentials for this profile before submission
            set_kaggle_credentials(profile['credential_name'])

            status = execute_single_script(
                script_path=job.script_path,
                username=profile['username'],
                title=f"{job.kernel_slug}",
                enable_gpu=enable_gpu,
                enable_internet=enable_internet,
                is_private=is_private,
                accelerator=accelerator
            )

            job.status = status
            if job.status == KernelStatus.FAILED:
                if job.retry_count < max_retries:
                    job.retry_count += 1
                    job.status = KernelStatus.PENDING
                    logger.info(
                        f"⟳ Retry {job.retry_count}/{max_retries} [{job.profile_name}]: {job.script_path}"
                    )
                else:
                    mark_failed(job_id)
                    logger.error(
                        f"✗ Failed permanently [{job.profile_name}]: {job.script_path}"
                    )
                return False
            
            profile['running'].add(job_id)
            logger.info(f"✓ Started [{job.profile_name}]: {job.script_path} (slug: {job.kernel_slug})")
            return True

        except Exception as e:
            logger.error(f"✗ Failed to submit [{job.profile_name}] {job.script_path}: {e}")
            if job.retry_count < max_retries:
                job.retry_count += 1
                job.status = KernelStatus.PENDING
                logger.info(
                    f"⟳ Retry {job.retry_count}/{max_retries} [{job.profile_name}]: {job.script_path}"
                )
            else:
                mark_failed(job_id)
                logger.error(
                    f"✗ Failed permanently [{job.profile_name}]: {job.script_path}"
                )
            return False

    def check_job_status(job_id: str) -> KernelStatus:
        """Check status of a running job. Swaps credentials before checking."""
        job = all_jobs[job_id]
        profile = profile_info[job.profile_name]

        try:
            # Swap credentials before checking status
            set_kaggle_credentials(profile['credential_name'])

            status_info = get_kaggle_kernel_status(job.kernel_slug, profile['username'])

            if status_info['is_complete']:
                return KernelStatus.COMPLETE
            elif status_info['has_error']:
                return KernelStatus.FAILED
            elif status_info['is_running']:
                return KernelStatus.RUNNING
            else:
                return KernelStatus.PENDING

        except Exception as e:
            logger.error(f"✗ Error checking status [{job.profile_name}] {job.script_path}: {e}")
            return KernelStatus.FAILED

    # Main execution loop
    while total_completed + total_failed < total_jobs:
        # Check status of all running jobs across all profiles
        for job_id in list(all_jobs.keys()):
            job = all_jobs[job_id]
            profile = profile_info[job.profile_name]

            if job_id in profile['running']:
                status = check_job_status(job_id)

                if status == KernelStatus.COMPLETE:
                    job.status = KernelStatus.COMPLETE
                    profile['running'].remove(job_id)
                    profile['completed'].add(job_id)
                    total_completed += 1
                    logger.info(f"✓ Completed [{job.profile_name}]: {job.script_path}")
                    notify_script_complete(job.script_path, job.kernel_slug)

                elif status == KernelStatus.FAILED:
                    job.status = KernelStatus.FAILED
                    profile['running'].remove(job_id)
                    
                    if job.retry_count < max_retries:
                        job.retry_count += 1
                        job.status = KernelStatus.PENDING
                        logger.info(f"⟳ Retry {job.retry_count}/{max_retries} [{job.profile_name}]: {job.script_path}")
                        notify_script_failed(job.script_path, job.kernel_slug, job.retry_count, max_retries)
                    else:
                        mark_failed(job_id)
                        logger.error(f"✗ Failed permanently [{job.profile_name}]: {job.script_path}")
                        notify_script_failed(job.script_path, job.kernel_slug, job.retry_count, max_retries)

        # Submit new jobs for each profile if slots available
        for profile_name, profile in profile_info.items():
            available_slots = profile['max_concurrent'] - len(profile['running'])

            if available_slots > 0:
                # Find pending jobs for this profile
                pending_jobs = [
                    job_id for job_id, job in all_jobs.items()
                    if job.profile_name == profile_name
                    and job.status == KernelStatus.PENDING
                    and job_id not in profile['running']
                ]

                # Submit up to available_slots jobs
                for job_id in pending_jobs[:available_slots]:
                    submit_job(job_id)

        # Wait before next check
        any_running = any(len(p['running']) > 0 for p in profile_info.values())

        if any_running:
            time.sleep(check_interval)

    # Summary
    print(f"\n{'='*70}")
    print(f"Multi-Profile Execution Summary:")
    print(f"{'='*70}")

    for profile_name, profile in profile_info.items():
        profile_total = len([j for j in all_jobs.values() if j.profile_name == profile_name])
        profile_completed = len(profile['completed'])
        profile_failed = len(profile['failed'])

        print(f"\nProfile: {profile_name}")
        print(f"  Completed: {profile_completed}/{profile_total}")
        print(f"  Failed: {profile_failed}/{profile_total}")

        if profile['failed']:
            print(f"  Failed scripts:")
            for job_id in profile['failed']:
                job = all_jobs[job_id]
                print(f"    - {job.script_path}")

    print(f"\n{'='*70}")
    print(f"Overall: Completed {total_completed}/{total_jobs}, Failed {total_failed}/{total_jobs}")
    print(f"{'='*70}\n")

    # Prepare failed scripts list for notification
    all_failed_scripts = [all_jobs[jid].script_path for jid in
                          [jid for p in profile_info.values() for jid in p['failed']]]

    notify_batch_summary(total_completed, total_failed, total_jobs, all_failed_scripts)


def set_kaggle_credentials(credential_name: str):
    """
    Set the active Kaggle credentials from the credential pool.

    Args:
        credential_name: Name of the credential file (without .json extension)
                        Will look for {credential_name}.json in utils/kaggle_credentials/

    Example:
        set_kaggle_credentials("bilpapster")  # Uses bilpapster.json
        set_kaggle_credentials("account2")    # Uses account2.json
    """
    import shutil

    # Current kaggle.json location
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True, mode=0o700)  # Ensure .kaggle directory exists with proper permissions
    current_file = kaggle_dir / "kaggle.json"

    # Credential file location in project
    project_root = Path(__file__).parent.parent  # Go up from utils/ to project root
    credentials_dir = project_root / "utils" / "kaggle_credentials"
    credential_file = credentials_dir / f"{credential_name}.json"

    # Validate credential file exists
    if not credential_file.exists():
        raise FileNotFoundError(f"Credential file not found at {credential_file}")

    try:
        # Copy credential to active location
        shutil.copy2(credential_file, current_file)

        # Set proper permissions (Kaggle requires 600)
        current_file.chmod(0o600)

        logger.info(f"Set Kaggle credentials to {credential_name}")

    except Exception as e:
        raise RuntimeError(f"Failed to set credentials: {e}")