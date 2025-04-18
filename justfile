# run:
# `just deploy team18@10.100.30.57:~/project`

deploy target:
    rsync -rah . \
        --exclude ".git" \
        --filter=':- .gitignore' \
        {{ target }}
