#install java
java -version || sudo yum insatll java 2>/dev/null || sudo apt-get install java || yum insatll java 2>/dev/null ||  apt-get install java
#download 
wget https://squizlabs.github.io/PHP_CodeSniffer/phpcs.phar ~/.jgrep.jar || curl https://squizlabs.github.io/PHP_CodeSniffer/phpcs.phar -o ~/.jgrep.jar 

echo "
alias jgrep='java -jar ~/.jgrep.jar'
"~/.bash_profile || echo "
alias jgrep='java -jar ~/.jgrep.jar'
"~/.profile
alias jgrep='java -jar ~/.jgrep.jar