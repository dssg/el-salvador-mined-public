if [ $# -eq 0 ]
  then
    echo "Missing port number argument. Please choose a number beteween 0-9."
  else
    source .venv/bin/activate
    jupyter notebook --no-browser --port=800"$1"
fi
