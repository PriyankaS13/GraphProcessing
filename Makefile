.PHONY: app-submit clean package-jar submit

app-submit: clean package-jar submit


clean:
	mvn clean

package-jar:
	mvn package

submit:
	spark-submit --class com.soundcloud.SocialNetwork ./target/socialnetwork-1.0-SNAPSHOT-jar-with-dependencies.jar ${input_file} ${degree} ./Output/
