package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// kafka consumer

func main() {
	consumer, err := sarama.NewConsumer([]string{"dymasun.work:9092"}, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	topics, err := consumer.Topics()
	if err != nil {
		fmt.Printf("fail to get topics, err:%v\n", err)
		return
	}
	fmt.Println(topics)
	for _, topic := range topics {
		fmt.Println(topic)
		partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
		if err != nil {
			fmt.Printf("fail to get list of partition:err%v\n", err)
			continue
		}
		for _, partition := range partitionList { // 遍历所有的分区
			// 针对每个分区创建一个对应的分区消费者
			pc, err := consumer.ConsumePartition("web_log", int32(partition), sarama.OffsetNewest)
			if err != nil {
				fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
				return
			}
			defer pc.AsyncClose()
			// 异步从每个分区消费信息
			messages := pc.Messages()
			msg := <-messages
			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
		}
	}
	// partitionList, err := consumer.Partitions("web_log") // 根据topic取到所有的分区
	// if err != nil {
	// 	fmt.Printf("fail to get list of partition:err%v\n", err)
	// 	return
	// }
	// fmt.Println(partitionList)
	// for partition := range partitionList { // 遍历所有的分区
	// 	// 针对每个分区创建一个对应的分区消费者
	// 	pc, err := consumer.ConsumePartition("web_log", int32(partition), sarama.OffsetNewest)
	// 	if err != nil {
	// 		fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
	// 		return
	// 	}
	// 	defer pc.AsyncClose()
	// 	// 异步从每个分区消费信息
	// 	go func(sarama.PartitionConsumer) {
	// 		for msg := range pc.Messages() {
	// 			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, msg.Value)
	// 		}
	// 	}(pc)
	// }
}
