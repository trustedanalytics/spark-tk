
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"

sparktkpackage=$MAINDIR/sparktkinstall

