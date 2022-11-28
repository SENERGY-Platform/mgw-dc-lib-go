/*
 * Copyright (c) 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mgw

import (
	"encoding/json"
	"errors"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"strings"
)

func (this *Client[T]) listenToDeviceCommands(deviceId string) error {
	if !this.mqtt.IsConnected() {
		log.Println("WARNING: mqtt client not connected")
		return errors.New("mqtt client not connected")
	}
	topic := "command/" + deviceId + "/+"

	handler := func(client paho.Client, message paho.Message) {
		if this.debug {
			log.Println("get command: \n", string(message.Payload()))
		}
		parts := strings.Split(message.Topic(), "/")
		serviceId := parts[len(parts)-1]

		command := Command{}
		err := json.Unmarshal(message.Payload(), &command)
		if err != nil {
			log.Println("ERROR: unable to unmarshal command", err)
			this.SendClientError("unable to unmarshal command: " + err.Error())
			return
		}
		this.handle(deviceId, serviceId, command)
	}

	token := this.mqtt.Subscribe(topic, 2, handler)
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Subscribe: ", topic, token.Error())
		return token.Error()
	}

	this.registerSubscription(topic, handler)
	return nil
}

func (this *Client[T]) stopListenToDeviceCommands(deviceId string) error {
	if !this.mqtt.IsConnected() {
		log.Println("WARNING: mqtt client not connected")
		return errors.New("mqtt client not connected")
	}
	topic := "command/" + deviceId + "/+"
	token := this.mqtt.Unsubscribe(topic)
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Unsubscribe: ", topic, token.Error())
		return token.Error()
	}
	this.unregisterSubscriptions(topic)
	return nil
}

func (this *Client[T]) handle(deviceId string, serviceId string, command Command) {
	var handler ServiceFunc[T]
	for i := range this.services {
		if this.services[i].LocalId == serviceId {
			handler = this.services[i].F
			break
		}
	}
	if handler == nil {
		errMsg := "unknown service" + serviceId
		log.Println("ERROR: " + errMsg)
		this.SendClientError(errMsg)
		return
	}
	device, ok := this.GetDevices()[deviceId]
	if !ok {
		errMsg := "unknown device" + deviceId
		log.Println("ERROR: " + errMsg)
		this.SendClientError(errMsg)
		return
	}
	var input interface{}
	err := json.Unmarshal([]byte(command.Data), &input)
	if err != nil {
		errMsg := "cannot unmarshal command data" + command.Data
		log.Println("ERROR: " + errMsg)
		this.SendCommandError(command.CommandId, errMsg)
		return
	}
	result, err := handler(device, input)
	if err != nil {
		log.Println("ERROR: " + err.Error())
		this.SendCommandError(command.CommandId, err.Error())
		return
	}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Println("ERROR: could not marshal results: " + err.Error())
		this.SendCommandError(command.CommandId, err.Error())
		return
	}
	command.Data = string(resultBytes)
	err = this.Respond(deviceId, serviceId, command)
	if err != nil {
		log.Println("ERROR: could not send response : " + err.Error())
		this.SendCommandError(command.CommandId, err.Error())
		return
	}
}
