package storage

import (
	"go.uber.org/zap"
)

func StartPersistentStorage(mqURL, sqlURL string) (PersistentStorageServer, error) {

	makePersistentStorageServer, err := MakePersistentStorageServer(mqURL, sqlURL)
	if err != nil {
		zap.L().Error("storage",
			zap.String("info", "make persistent storage failed"),
			zap.Error(err))
		return nil, err
	}
	return makePersistentStorageServer, nil
}
