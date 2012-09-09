#!/bin/sh

# Cleans Mercurial working copy by removing all the ignored files
# except of the local project files.

# Required package: trash-cli
TRASH="trash-put"

# We have to have the trash utility installed
if ! ($TRASH --version >/dev/null); then
    echo "Failed to invoke the trash command line utility"
    echo "Is the trash-cli package installed?"
    exit 1
fi


# List ignored files
IGNORED_FILES=`hg status -in`
if [ $? -ne 0 ]; then
    echo ""
    echo "Mercurial failed to list the ignored files."
    echo ""
    echo "Are you running this script inside a Mercurial working copy?"
    exit 2
fi

# Preserve some local configuration files
FILES_TO_REMOVE=`hg status -in | grep -v ".wpu" | grep -v "etc/config.py"`

# Is the working copy already clean?
if [ -z "$FILES_TO_REMOVE" ]; then
    echo "Your working copy is already clean."
    exit 0
fi

echo "Cleaning up working copy by trashing the following files:"
for FN in $FILES_TO_REMOVE; do
    echo "$FN"
done
$TRASH $FILES_TO_REMOVE

echo "Done."
