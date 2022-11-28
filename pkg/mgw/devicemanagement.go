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
	"log"
)

func (this *Client[T]) SetDevice(device T) (err error) {
	info := device.GetInfo()
	switch info.State {
	case Online:
		err = this.listenToDeviceCommands(info.Id)
		break
	case Offline:
		err = this.stopListenToDeviceCommands(info.Id)
		break
	default:
		err = errors.New("illegale device.GetInfo().State")
	}
	if err != nil {
		return err
	}
	err = this.sendDeviceUpdate(DeviceInfoUpdate{
		Method:   "set",
		DeviceId: info.Id,
		Data:     info,
	})
	if err != nil {
		return err
	}
	this.devicesMux.Lock()
	this.devices[info.Id] = device
	this.devicesMux.Unlock()
	return
}

// DeleteDevice Permanently deletes the device. If the device is only gone temporarily use SetDevice with updated DeviceInfo instead!
func (this *Client[T]) DeleteDevice(deviceId string) (err error) {
	err = this.stopListenToDeviceCommands(deviceId)
	if err != nil {
		return err
	}
	err = this.sendDeviceUpdate(DeviceInfoUpdate{
		Method:   "delete",
		DeviceId: deviceId,
	})
	if err != nil {
		return err
	}
	this.devicesMux.Lock()
	delete(this.devices, deviceId)
	this.devicesMux.Unlock()
	return
}

func (this *Client[T]) GetDevices() (devices map[string]T) {
	return this.devices
}

func (this *Client[T]) sendDeviceUpdate(info DeviceInfoUpdate) error {
	if !this.mqtt.IsConnected() {
		log.Println("WARNING: mqtt client not connected")
		return errors.New("mqtt client not connected")
	}
	topic := DeviceManagerTopic + "/" + this.connectorId
	msg, err := json.Marshal(info)
	if this.debug {
		log.Println("DEBUG: publish ", topic, string(msg))
	}
	token := this.mqtt.Publish(topic, 2, false, string(msg))
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Client.Publish(): ", token.Error())
		return token.Error()
	}
	return err
}
