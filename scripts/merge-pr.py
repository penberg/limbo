#!/usr/bin/env python3
#
# A script to merge Limbo pull requests with a nice merge commit.
#
# Requirements:
#
# ```
# pip install PyGithub
# ```

import sys
import re
from github import Github
import os
import subprocess

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode('utf-8').strip(), error.decode('utf-8').strip(), process.returncode

def get_pr_info(repo, pr_number):
    pr = repo.get_pull(int(pr_number))
    author = pr.user
    author_name = author.name if author.name else author.login
    return {
        'number': pr.number,
        'title': pr.title,
        'author': author_name,
        'head': pr.head.ref,
        'body': pr.body.strip() if pr.body else ''
    }

def merge_pr(pr_number):
    # GitHub authentication
    token = os.getenv('GITHUB_TOKEN')

    g = Github(token)
    
    # Get the repository
    repo_name = os.getenv('GITHUB_REPOSITORY')
    if not repo_name:
        print("Error: GITHUB_REPOSITORY environment variable not set")
        sys.exit(1)
    
    repo = g.get_repo(repo_name)
    
    # Get PR information
    pr_info = get_pr_info(repo, pr_number)
    
    # Format commit message
    commit_message = f"Merge '{pr_info['title']}' from {pr_info['author']}\n\n{pr_info['body']}\n\nCloses #{pr_info['number']}"
    
    # Fetch the PR branch
    cmd = f"git fetch origin pull/{pr_number}/head:{pr_info['head']}"
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error fetching PR branch: {error}")
        sys.exit(1)
    
    # Checkout main branch
    cmd = "git checkout main"
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error checking out main branch: {error}")
        sys.exit(1)
    
    # Merge the PR
    cmd = f"git merge --no-ff {pr_info['head']} -m \"{commit_message}\""
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error merging PR: {error}")
        sys.exit(1)
    
    print("Pull request merged successfully!")
    print(f"Merge commit message:\n{commit_message}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python merge_pr.py <pr_number>")
        sys.exit(1)
    
    pr_number = sys.argv[1]
    if not re.match(r'^\d+$', pr_number):
        print("Error: PR number must be a positive integer")
        sys.exit(1)
    
    merge_pr(pr_number)
