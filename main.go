package main

var AppConfigs AppConfig

func main() {

	AppConfigs = LoadConfigs()
	if AppConfigs.mode == PRODUCER_MODE {
		ProducerWorkFlow()
	} else {
		ConsumerWorkFlow()
	}
}
