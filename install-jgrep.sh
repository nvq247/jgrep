#install java
java -version || sudo yum insatll java 2>/dev/null || sudo apt-get install java || yum insatll java 2>/dev/null ||  apt-get install java
#download 
wget https://raw.githubusercontent.com/nvq247/jgrep/master/jgrep.jar ~/.jgrep.jar || curl https://raw.githubusercontent.com/nvq247/jgrep/master/jgrep.jar -o ~/.jgrep.jar 

echo "
alias jgrep='java -jar ~/.jgrep.jar'
"~/.bash_profile || echo "
alias jgrep='java -jar ~/.jgrep.jar'
"~/.profile
alias jgrep='java -jar ~/.jgrep.jar