# Launch and connect to an EC2 instance for the carbon flux model over SSM (no OpenVPN). See "Checking that a new run environment gives the same results" in readme.md.
# Needs the AWS CLI, the Session Manager plugin, and `aws sso login --profile ForestResearcher`.

export AWS_PROFILE := env_var_or_default("AWS_PROFILE", "ForestResearcher")
export AWS_REGION := "us-east-1"

template := "carbon_flux_model_python3_v2"
subnet_name := "GFW subnet zone us-east-1c"
# Added at launch so SSM can manage the instance. Only grants SSM permissions; the template itself is unchanged.
ssm_instance_profile := "AmazonSSMRoleForInstancesQuickSetup"
name_tag := "carbon-flux-model-" + env_var("USER")
project_tag := "carbon-budget"
# The screen session on the instance that `just container` runs in.
session := "carbon-budget"

# List the commands
default:
    @just --list

# Launch from the template. Version 29 = spot (test runs), 30 = on demand (full runs). Types: r5d.xlarge for a few tiles, r5d.24xlarge for all.
launch version="29" type="r5d.xlarge":
    #!/usr/bin/env bash
    set -euo pipefail
    subnet=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values={{subnet_name}}" \
      --query 'Subnets[0].SubnetId' --output text)
    # wri:owner is your SSO user name (your email), from the end of your role session ARN.
    user=$(aws sts get-caller-identity --query Arn --output text | awk -F/ '{print tolower($NF)}')
    tags="{Key=Name,Value={{name_tag}}},{Key=wri:project,Value={{project_tag}}},{Key=wri:owner,Value=$user}"
    echo "Launching {{type}} from {{template}} v{{version}} in {{subnet_name}} ($subnet)"
    echo "Tags: Name={{name_tag}}, wri:project={{project_tag}}, wri:owner=$user"
    id=$(aws ec2 run-instances \
      --launch-template 'LaunchTemplateName={{template}},Version={{version}}' \
      --instance-type '{{type}}' \
      --subnet-id "$subnet" \
      --iam-instance-profile 'Name={{ssm_instance_profile}}' \
      --tag-specifications "ResourceType=instance,Tags=[$tags]" "ResourceType=volume,Tags=[$tags]" \
      --query 'Instances[0].InstanceId' --output text)
    echo "Instance $id launched. Waiting for it to run..."
    aws ec2 wait instance-running --instance-ids "$id"
    echo "Running. Waiting for it to register with SSM (a few minutes)..."
    for _ in $(seq 60); do
      status=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
        --query 'InstanceInformationList[0].PingStatus' --output text)
      [ "$status" = "Online" ] && break
      sleep 10
    done
    [ "$status" = "Online" ] || { echo "Not online in SSM after 10 minutes."; exit 1; }
    echo "Ready. Connect with: just connect $id"
    echo "The template's setup (disks, Docker, git clone) may still be running; on the instance,"
    echo "/var/lib/cloud/instance/boot-finished exists when it's done."
    echo "When finished: just terminate $id"

# Launch, set up `ssh cfm`, send your credentials, then build a carbon-budget branch and open the container. Version and type as for launch.
up branch version="29" type="r5d.xlarge":
    #!/usr/bin/env bash
    set -euo pipefail
    # Check the branch exists before paying for an instance.
    git ls-remote --exit-code --heads https://github.com/wri/carbon-budget {{quote(branch)}} >/dev/null \
      || { echo "No branch '{{branch}}' on github.com/wri/carbon-budget."; exit 1; }
    {{just_executable()}} launch '{{version}}' '{{type}}'
    {{just_executable()}} ssh-config
    {{just_executable()}} creds
    {{just_executable()}} container {{quote(branch)}}

# Set up `ssh cfm` (also scp, rsync, VS Code) over SSM for an instance. Rerun for each new instance.
ssh-config id="":
    #!/usr/bin/env bash
    set -euo pipefail
    id=$(just _instance "{{id}}")
    key="$HOME/.ssh/carbon-flux-model"
    conf="$HOME/.ssh/carbon-flux-model.config"
    include="Include $conf"

    # 1. A key pair just for these instances (the shared r5d_ec2 key isn't needed).
    [ -f "$key" ] || ssh-keygen -q -t ed25519 -N "" -C "carbon-flux-model $USER" -f "$key"

    # 2. Add the public key to ec2-user on the instance, through SSM. Public keys aren't secret.
    b64=$(base64 < "$key.pub" | tr -d '\n')
    ak=/home/ec2-user/.ssh/authorized_keys
    script="echo $b64 | base64 -d > /tmp/k.pub; grep -qxFf /tmp/k.pub $ak || cat /tmp/k.pub >> $ak; rm /tmp/k.pub"
    echo "Adding $key.pub to ec2-user on $id..."
    cmd=$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
      --comment "Add SSH key for $USER" --parameters "{\"commands\":[\"$script\"]}" \
      --query Command.CommandId --output text)
    aws ssm wait command-executed --command-id "$cmd" --instance-id "$id" \
      || { aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
             --query '[Status,StandardErrorContent]' --output text; exit 1; }

    # 3. The SSH config entry, in its own file. ForwardAgent is off so the shared instance can't use your keys.
    cat > "$conf" <<EOF
    # Written by "just ssh-config" in carbon-budget; rewritten on each run.
    Host cfm
      HostName $id
      User ec2-user
      IdentityFile $key
      IdentitiesOnly yes
      ForwardAgent no
      # Each instance is new, so accept its host key the first time (SSM already reaches the right instance by ID).
      # Kept apart from ~/.ssh/known_hosts so old instances don't pile up there.
      StrictHostKeyChecking accept-new
      UserKnownHostsFile ~/.ssh/carbon-flux-model.known_hosts
      ProxyCommand $(command -v aws) ssm start-session --profile $AWS_PROFILE --region $AWS_REGION --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p
    EOF
    echo "Wrote $conf (Host cfm -> $id)"

    # 4. Include that file from ~/.ssh/config, at the top so it takes priority over `Host *`.
    if ! grep -qxF "$include" "$HOME/.ssh/config" 2>/dev/null; then
      read -r -p "Add '$include' to the top of ~/.ssh/config (after backing it up)? [y/N] " answer
      if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
        if [ -f "$HOME/.ssh/config" ]; then
          backup="$HOME/.ssh/config.bak-$(date +%Y%m%d-%H%M%S)"
          cp -p "$HOME/.ssh/config" "$backup"
          { echo "$include"; echo; cat "$backup"; } > "$HOME/.ssh/config"
          echo "Added. Backup: $backup"
        else
          echo "$include" > "$HOME/.ssh/config"; chmod 600 "$HOME/.ssh/config"
        fi
      else
        echo "Not added. Put this line at the top of ~/.ssh/config yourself: $include"
      fi
    fi
    echo "Connect with: ssh cfm"

# Send your SSO credentials to the instance (over `ssh cfm`) so Docker can use S3. They expire with your SSO session: rerun, then restart the container.
creds host="cfm":
    #!/usr/bin/env bash
    set -euo pipefail
    file=.carbon-budget.env
    # Temporary credentials (key, secret, session token), as KEY=VALUE lines that `docker run --env-file` reads.
    env=$(aws configure export-credentials --format env-no-export) \
      || { echo "No credentials. Run: aws sso login --profile $AWS_PROFILE"; exit 1; }
    expires=$(echo "$env" | sed -n 's/^AWS_CREDENTIAL_EXPIRATION=//p')
    # Only you (ec2-user) can read the file; `just terminate` deletes it with the instance.
    echo "$env" | ssh '{{host}}' "umask 077; cat > ~/$file"
    echo "Wrote ~/$file on {{host}} ($AWS_PROFILE credentials, expire ${expires:-at the end of your SSO session})."

# Check out a branch, build the Docker image, and open the container, in screen on the instance (`just attach` to get back). Needs `just creds` first.
container branch:
    #!/usr/bin/env bash
    set -euo pipefail
    # On the instance: check the credentials (the container reads them once, at start), check there isn't a session
    # already, install screen if needed, and write the script the session runs.
    ssh cfm 'set -e
      exp=$(sed -n "s/^AWS_CREDENTIAL_EXPIRATION=//p" ~/.carbon-budget.env 2>/dev/null || true)
      if [ -z "$exp" ] || [ "$(date -d "$exp" +%s)" -le "$(date +%s)" ]; then
        echo "No credentials on the instance, or they have expired. Run: just creds" >&2; exit 1
      fi
      if screen -ls 2>/dev/null | grep -q "[.]{{session}}[[:space:]]"; then
        echo "The {{session}} screen session is already running. Reattach with: just attach" >&2; exit 1
      fi
      command -v screen >/dev/null || sudo yum install -y -q screen
      # Scrollback goes to the terminal, so the mouse wheel and scrollbar work.
      [ -f ~/.screenrc ] || printf "startup_message off\ndefscrollback 10000\ntermcapinfo xterm* ti@:te@\n" > ~/.screenrc
      cat > ~/{{session}}.sh' <<'EOF'
    #!/bin/bash
    # Written by "just container" in carbon-budget. Runs in the screen session; the steps follow readme.md ("Building the Docker container").
    branch=$1
    if [ ! -f /var/lib/cloud/instance/boot-finished ]; then
      echo "Waiting for the template setup (disks, Docker, git clone) to finish..."
      until [ -f /var/lib/cloud/instance/boot-finished ]; do sleep 10; done
    fi
    cd ~/carbon-budget &&
      git fetch --quiet origin &&
      git checkout "$branch" &&
      git pull --ff-only &&
      sudo docker build . -t gfw/carbon-budget &&
      sudo docker run --rm -it --env-file ~/.carbon-budget.env gfw/carbon-budget
    echo "Exited with code $?. This shell is still in screen: exit to close the session, or Ctrl-a d to detach."
    exec bash -l
    EOF
    echo "Starting screen session {{session}}. Detach with Ctrl-a d; if the connection drops, reattach with: just attach"
    ssh -t cfm "screen -S {{session}} bash ~/{{session}}.sh {{quote(branch)}}" \
      || { echo "Disconnected. The session keeps running; reattach with: just attach"; exit 1; }

# Reattach to the screen session from `just container` (e.g. after the connection drops).
attach:
    ssh -t cfm screen -d -r {{session}} || echo "No {{session}} session to reattach to. Start one with: just container <branch>"

# Copy the launch template, unchanged, to the account of another AWS profile (e.g. `just copy-template land`). Versions are renumbered from 1.
copy-template to_profile versions="29 30":
    #!/usr/bin/env bash
    set -euo pipefail
    from_account=$(aws sts get-caller-identity --query Account --output text)
    to_account=$(aws sts get-caller-identity --profile '{{to_profile}}' --query Account --output text)
    echo "Copying {{template}} versions {{versions}} from $from_account ($AWS_PROFILE) to $to_account ({{to_profile}})"
    [ "$from_account" != "$to_account" ] || { echo "Both profiles are in account $from_account."; exit 1; }
    if aws ec2 describe-launch-templates --profile '{{to_profile}}' --launch-template-names '{{template}}' >/dev/null 2>&1; then
      echo "{{template}} already exists in $to_account. Not overwriting it."; exit 1
    fi

    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    new=1
    for v in {{versions}}; do
      # The template's settings, as one JSON document (user data stays base64-encoded, as the API expects).
      aws ec2 describe-launch-template-versions --launch-template-name '{{template}}' --versions "$v" \
        --query 'LaunchTemplateVersions[0].LaunchTemplateData' --output json > "$tmp/data.json"
      old_desc=$(aws ec2 describe-launch-template-versions --launch-template-name '{{template}}' --versions "$v" \
        --query 'LaunchTemplateVersions[0].VersionDescription' --output text)
      desc="Copy of $from_account v$v: $old_desc"
      if [ "$new" = 1 ]; then
        aws ec2 create-launch-template --profile '{{to_profile}}' --launch-template-name '{{template}}' \
          --version-description "$desc" --launch-template-data "file://$tmp/data.json" --output text \
          --query 'LaunchTemplate.LaunchTemplateId' > "$tmp/id"
      else
        aws ec2 create-launch-template-version --profile '{{to_profile}}' --launch-template-name '{{template}}' \
          --version-description "$desc" --launch-template-data "file://$tmp/data.json" >/dev/null
      fi
      echo "  v$v -> v$new ($old_desc)"
      new=$((new + 1))
    done
    echo "Created $(cat "$tmp/id") in $to_account. Its default version is v1 (a copy of v$(echo {{versions}} | cut -d' ' -f1))."

    # The copy is exact, so anything that only exists in the source account won't resolve in the new one.
    echo "Checking what the copy refers to in $to_account:"
    for v in {{versions}}; do
      aws ec2 describe-launch-template-versions --launch-template-name '{{template}}' --versions "$v" \
        --query 'LaunchTemplateVersions[0].LaunchTemplateData.[ImageId, KeyName, join(`,`, SecurityGroupIds || `[]`)]' \
        --output text
    done | sort -u | while read -r ami key sgs; do
      aws ec2 describe-images --profile '{{to_profile}}' --image-ids "$ami" >/dev/null 2>&1 \
        && echo "  ok      AMI $ami" || echo "  MISSING AMI $ami"
      if [ "$key" != "None" ]; then
        aws ec2 describe-key-pairs --profile '{{to_profile}}' --key-names "$key" >/dev/null 2>&1 \
          && echo "  ok      key pair $key" || echo "  MISSING key pair $key"
      fi
      for sg in ${sgs//,/ }; do
        aws ec2 describe-security-groups --profile '{{to_profile}}' --group-ids "$sg" >/dev/null 2>&1 \
          && echo "  ok      security group $sg" || echo "  MISSING security group $sg"
      done
    done
    echo "Launches fail until anything MISSING is fixed (a new template version with the right IDs)."

# Open a shell on the instance. Without an ID, uses your newest instance from `just launch`.
connect id="":
    #!/usr/bin/env bash
    set -euo pipefail
    id=$(just _instance "{{id}}")
    echo "Connecting to $id. Sessions start as ssm-user; run 'sudo su - ec2-user' to get to ~/carbon-budget."
    aws ssm start-session --target "$id"

# Terminate the instance (asks first). Without an ID, uses your newest instance from `just launch`. Only terminates instances named carbon-flux-model-<your username>.
terminate id="":
    #!/usr/bin/env bash
    set -euo pipefail
    id=$(just _instance "{{id}}")
    name=$(aws ec2 describe-instances --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].Tags[?Key==`Name`].Value | [0]' --output text)
    [ "$name" = "{{name_tag}}" ] || { echo "$id is named '$name', not {{name_tag}}. Not terminating."; exit 1; }
    read -r -p "Terminate $id ($name)? Its local disks, and any results not uploaded, are lost. [y/N] " answer
    [ "$answer" = "y" ] || [ "$answer" = "Y" ] || { echo "Cancelled."; exit 0; }
    aws ec2 terminate-instances --instance-ids "$id" \
      --query 'TerminatingInstances[0].[InstanceId,CurrentState.Name]' --output text

# Prints the given instance ID, or your newest instance named {{name_tag}} that isn't terminated.
[private]
_instance id="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "{{id}}" ]; then echo "{{id}}"; exit 0; fi
    id=$(aws ec2 describe-instances \
      --filters "Name=tag:Name,Values={{name_tag}}" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'sort_by(Reservations[].Instances[], &LaunchTime)[-1].InstanceId' --output text)
    [ "$id" != "None" ] || { echo "No instance named {{name_tag}}. Run: just launch" >&2; exit 1; }
    echo "$id"
