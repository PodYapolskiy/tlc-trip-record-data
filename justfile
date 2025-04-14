# run:
# `just deploy team18@10.100.30.57:~/project`

deploy target:
    rsync -r --info=progress2 -ahv . \
        --exclude ".git" \
        --filter=':- .gitignore' \
        {{ target }}
