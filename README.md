# install jgrep
wget https://raw.githubusercontent.com/nvq247/jgrep/master/install-jgrep.sh -O install-jgrep.sh && bash install-jgrep.sh

curl https://raw.githubusercontent.com/nvq247/jgrep/master/install-jgrep.sh -o install-jgrep.sh && bash install-jgrep.sh

#expamle 1
cat file|jgrep '(\d+)(\w+)' '$2  $1'
#expamle 2
cat file|jgrep "(\\d+)(\\w+)" "\$2"
