package storage

import "log"

func StartPersistentStorage(mqURL, sqlURL string) PersistentStorageServer {

	makePersistentStorageServer, err := MakePersistentStorageServer(mqURL, sqlURL)
	if err != nil {
		log.Printf("MakeMQServer ERR: " + err.Error())
		return nil
	}
	return makePersistentStorageServer
}
