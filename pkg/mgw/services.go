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
	"golang.org/x/exp/maps"
	"log"
	"sync"
	"time"
)

func (this *Client[T]) RegisterServiceStruct(service Service[T]) {
	this.RegisterService(service.LocalId, service.F, service.D)
}

func (this *Client[T]) RegisterService(serviceLocalId string, f ServiceFunc[T], d *time.Duration) {
	this.servicesMux.Lock()
	this.services = append(this.services, Service[T]{
		F:       f,
		D:       d,
		LocalId: serviceLocalId,
	})
	if d != nil {
		go func() {
			init := false
			for {
				var timer *time.Timer
				if init {
					timer = time.NewTimer(*d)
				} else {
					timer = time.NewTimer(0)
					init = true
				}
				select {
				case <-timer.C:
					devices := maps.Values(this.GetDevices())
					eventWg := sync.WaitGroup{}
					for _, d := range devices {
						if d.GetInfo().State == Offline {
							continue
						}
						d := d
						eventWg.Add(1)
						go func() {
							defer eventWg.Done()
							result, err := f(d, nil)
							if err != nil {
								errMsg := "event for service " + serviceLocalId + " got error " + err.Error()
								log.Println("ERROR: " + errMsg)
								this.SendDeviceError(d.GetInfo().Id, errMsg)
								return
							}
							resultBytes, err := json.Marshal(result)
							if err != nil {
								errMsg := "result for service " + serviceLocalId + " could not be marshalled " + err.Error()
								log.Println("ERROR: " + errMsg)
								this.SendDeviceError(d.GetInfo().Id, errMsg)
								return
							}
							err = this.SendEvent(d.GetInfo().Id, serviceLocalId, resultBytes)
							if err != nil {
								errMsg := "result for service " + serviceLocalId + " could not be sent " + err.Error()
								log.Println("ERROR: " + errMsg)
								this.SendClientError(errMsg)
								return
							}
						}()
					}
					eventWg.Wait() // dont start new event routines when old ones still running
				case <-this.ctx.Done():
					return
				}
			}
		}()
	}
}
