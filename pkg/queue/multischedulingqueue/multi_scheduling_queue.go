/*
 Copyright 2021 The Kube-Queue Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package multischedulingqueue

import (
	"context"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"sort"
	"sync"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/queue"
	"github.com/kube-queue/kube-queue/pkg/queue/schedulingqueue"
	schedv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
)

// Making sure that MultiSchedulingQueue implements MultiSchedulingQueue.
var _ queue.MultiSchedulingQueue = &MultiSchedulingQueue{}

type MultiSchedulingQueue struct {
	sync.RWMutex
	fw       framework.Framework
	queueMap map[string]queue.SchedulingQueue
	lessFunc framework.MultiQueueLessFunc
	podInitialBackoffSeconds int
	podMaxBackoffSeconds int
	omissiveQu map[string]interface{}
}

func NewMultiSchedulingQueue(fw framework.Framework, podInitialBackoffSeconds int, podMaxBackoffSeconds int, informersFactory informers.SharedInformerFactory) (queue.MultiSchedulingQueue, error) {

	mq := &MultiSchedulingQueue{
		fw:       fw,
		queueMap: make(map[string]queue.SchedulingQueue),
		lessFunc: fw.MultiQueueSortFunc(),
		podInitialBackoffSeconds: podInitialBackoffSeconds,
		podMaxBackoffSeconds: podMaxBackoffSeconds,
		omissiveQu: make(map[string]interface{}),
	}
    // TODO 该处需要研究一下informer
	//nsList, err := informersFactory.Core().V1().Namespaces().Lister().List(labels.Everything())
	//if err != nil {
	//	return nil, err
	//}
	//// support mutil queue, key is namespace name
	//for _, ns := range nsList {
	//	// Check whether the Namespace needs to be processed
	//	if b := regexp.MustCompile(utils.RegexpStr).MatchString(ns.Name); !b {
	//		continue
	//	}
	//	nsQueue := schedulingqueue.NewPrioritySchedulingQueue(fw, ns.Name, priority.Name, podInitialBackoffSeconds, podMaxBackoffSeconds)
	//	mq.queueMap[ns.Name] = nsQueue
	//}
	return mq, nil
}

func (mq *MultiSchedulingQueue) Run() {
	mq.Lock()
	defer mq.Unlock()

	for _, q := range mq.queueMap {
		q.Run()
	}
}

func (mq *MultiSchedulingQueue) Close() {
	mq.Lock()
	defer mq.Unlock()

	for _, q := range mq.queueMap {
		q.Close()
	}
}

func (mq *MultiSchedulingQueue) Add(q *v1alpha1.Queue) error {
	mq.Lock()
	defer mq.Unlock()

	pq := schedulingqueue.NewPrioritySchedulingQueue(mq.fw, q.Name, "Priority", mq.podInitialBackoffSeconds, mq.podMaxBackoffSeconds)
	mq.queueMap[pq.Name()] = pq
	return nil
}

func (mq *MultiSchedulingQueue) Delete(q *v1alpha1.Queue) error {
	mq.Lock()
	defer mq.Unlock()

	delete(mq.queueMap, q.Name)
	return nil
}

func (mq *MultiSchedulingQueue) Update(old *v1alpha1.Queue, new *v1alpha1.Queue) error {
	pq := schedulingqueue.NewPrioritySchedulingQueue(mq.fw, new.Name, "Priority", mq.podInitialBackoffSeconds, mq.podMaxBackoffSeconds)
	mq.queueMap[pq.Name()] = pq
	return nil
}

func (mq *MultiSchedulingQueue) GetQueueByName(name string) (queue.SchedulingQueue, bool) {
	mq.Lock()
	defer mq.Unlock()

	if name == "" {
		return nil, false
	}
	q, ok := mq.queueMap[name]
	return q, ok
}

func (mq *MultiSchedulingQueue) SortedQueue() []queue.SchedulingQueue {
	mq.Lock()
	defer mq.Unlock()

	len := len(mq.queueMap)
	unSortedQueue := make([]queue.SchedulingQueue, len)
	index := 0
	for _, q := range mq.queueMap {
		unSortedQueue[index] = q
		index++
	}
	sort.Slice(unSortedQueue, func(i, j int) bool {
		return mq.lessFunc(unSortedQueue[i].QueueInfo(), unSortedQueue[j].QueueInfo())
	})
	return unSortedQueue
}

func queueInfoKeyFunc(obj interface{}) (string, error) {
	q := obj.(queue.SchedulingQueue)
	return q.Name(), nil
}

func (mq *MultiSchedulingQueue) EnOmissiveQu(q *schedv1alpha1.QueueUnit) {
	mq.Lock()
	defer mq.Unlock()

	mq.omissiveQu[q.Namespace + "/" + q.Name] = q
}

func (mq *MultiSchedulingQueue) DeOmissiveQu(q *schedv1alpha1.QueueUnit) {
	mq.Lock()
	defer mq.Unlock()

	delete(mq.omissiveQu, q.Namespace + "/" + q.Name)
}

func (mq *MultiSchedulingQueue) UpdateOmissiveQu(old *schedv1alpha1.QueueUnit,new *schedv1alpha1.QueueUnit) {
	mq.Lock()
	defer mq.Unlock()

	if _, ok := mq.omissiveQu[old.Namespace + "/" + old.Name]; !ok {
		return
	}
	mq.omissiveQu[new.Namespace + "/" + new.Name] = new
}

func (mq *MultiSchedulingQueue) HandleOmissiveQu() {
	go wait.Until(mq.flushOmissiveQu, 1.0*time.Second, context.TODO().Done())
}

func (mq *MultiSchedulingQueue) flushOmissiveQu() {
	mq.Lock()
	defer mq.Unlock()

	if len(mq.omissiveQu) == 0 {
		return
	}
	for k, v := range mq.omissiveQu {
		qu := v.(*v1alpha1.QueueUnit)
		q, ok := mq.GetQueueByName(qu.Namespace)
		if !ok {
			continue
		}
		err := q.Add(qu)
		if err == nil {
			delete(mq.omissiveQu, k)
		}
	}
}







