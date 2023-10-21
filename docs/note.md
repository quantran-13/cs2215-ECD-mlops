clearml-agent daemon --queue default --docker python:3.11.6 --detached --force-current-version --cpu-only



clearml-agent daemon --queue default --detached

ps -ef | grep clearml-agent
