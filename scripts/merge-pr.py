#!/usr/bin/env python3
#
# Copyright 2024 the Limbo authors. All rights reserved. MIT license.
#
# A script to merge a pull requests with a nice merge commit.
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
import tempfile
import textwrap


def run_command(command):
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode('utf-8').strip(), error.decode('utf-8').strip(), process.returncode


def get_user_email(g, username):
    try:
        user = g.get_user(username)
        if user.email:
            return user.email
        # If public email is not available, try to get from events
        events = user.get_events()
        for event in events:
            if event.type == "PushEvent" and event.payload.get("commits"):
                for commit in event.payload["commits"]:
                    if commit.get("author") and commit["author"].get("email"):
                        return commit["author"]["email"]
    except Exception as e:
        print(f"Error fetching email for user {username}: {str(e)}")

    # If we couldn't find an email, return a noreply address
    return f"{username}@users.noreply.github.com"


def get_pr_info(g, repo, pr_number):
    pr = repo.get_pull(int(pr_number))
    author = pr.user
    author_name = author.name if author.name else author.login

    # Get the list of users who reviewed the PR
    reviewed_by = []
    reviews = pr.get_reviews()
    for review in reviews:
        if review.state == 'APPROVED':
            reviewer = review.user
            reviewer_name = reviewer.name if reviewer.name else reviewer.login
            reviewer_email = get_user_email(g, reviewer.login)
            reviewed_by.append(f"{reviewer_name} <{reviewer_email}>")

    return {
        'number': pr.number,
        'title': pr.title,
        'author': author_name,
        'head': pr.head.ref,
        'body': pr.body.strip() if pr.body else '',
        'reviewed_by': reviewed_by
    }


def wrap_text(text, width=72):
    lines = text.split('\n')
    wrapped_lines = []
    in_code_block = False
    for line in lines:
        if line.strip().startswith('```'):
            in_code_block = not in_code_block
            wrapped_lines.append(line)
        elif in_code_block:
            wrapped_lines.append(line)
        else:
            wrapped_lines.extend(textwrap.wrap(line, width=width))
    return '\n'.join(wrapped_lines)


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
    pr_info = get_pr_info(g, repo, pr_number)

    # Format commit message
    commit_title = f"Merge '{pr_info['title']}' from {pr_info['author']}"
    commit_body = wrap_text(pr_info['body'])

    commit_message = f"{commit_title}\n\n{commit_body}\n"

    # Add Reviewed-by lines
    for approver in pr_info['reviewed_by']:
        commit_message += f"\nReviewed-by: {approver}"

    # Add Closes line
    commit_message += f"\n\nCloses #{pr_info['number']}"

    # Create a temporary file for the commit message
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(commit_message)
        temp_file_path = temp_file.name

    # Fetch the PR branch
    cmd = f"git fetch origin pull/{pr_number}/head:{pr_info['head']}"
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error fetching PR branch: {error}")
        os.unlink(temp_file_path)
        sys.exit(1)

    # Checkout main branch
    cmd = "git checkout main"
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error checking out main branch: {error}")
        os.unlink(temp_file_path)
        sys.exit(1)

    # Merge the PR
    cmd = f"git merge --no-ff {pr_info['head']} -F {temp_file_path}"
    output, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error merging PR: {error}")
        os.unlink(temp_file_path)
        sys.exit(1)

    # Clean up the temporary file
    os.unlink(temp_file_path)

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
