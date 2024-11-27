package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"time"

	"github.com/jessevdk/go-flags"
	"tgragnato.it/magnetico/dht"
	"tgragnato.it/magnetico/dht/mainline"
	"tgragnato.it/magnetico/metadata"
	"tgragnato.it/magnetico/persistence"
	"tgragnato.it/magnetico/stats"
	"tgragnato.it/magnetico/storage"
	"tgragnato.it/magnetico/utils"
	"tgragnato.it/magnetico/web"
)

var opFlags struct {
	RunDaemon  bool
	RunWeb     bool
	RunStorage bool

	QueueServiceUrl                 string
	QueueServicePersistsDatabaseURL string
	DatabaseURL                     string

	IndexerAddrs        []string
	IndexerMaxNeighbors uint

	LeechMaxN              int
	BootstrappingNodes     []string
	BootstrapNodesSelfPort bool
	FilterNodesCIDRs       []net.IPNet

	Addr string

	Credentials     map[string][]byte
	CredentialsPath string

	IsDevEnv bool
	LogLevel zapcore.Level
}

func main() {
	// opFlags is the "operational flags"
	if parseFlags() != nil {
		// Do not print any error messages as jessevdk/go-flags already did.
		return
	}

	// zap log
	logger := utils.NewZapLog(opFlags.LogLevel, opFlags.IsDevEnv)
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// Handle Ctrl-C gracefully.
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt)

	// Reload credentials when you receive SIGHUP
	sighupChan := make(chan os.Signal, 1)
	signal.Notify(sighupChan, syscall.SIGHUP)
	go func() {
		for range sighupChan {
			if opFlags.Credentials == nil {
				// Ignoring SIGHUP since no credential file was supplied
				continue
			}

			opFlags.Credentials = make(map[string][]byte)
			if err := loadCred(opFlags.CredentialsPath); err != nil {
				zap.L().Fatal("main",
					zap.String("info", "couldn't load credentials"),
					zap.Error(err))
			}
		}
	}()

	database, err := persistence.MakeDatabase(opFlags.DatabaseURL)
	if err != nil {
		zap.L().Fatal("main",
			zap.String("info", "Could not open the database. "+opFlags.DatabaseURL),
			zap.Error(err))
	}
	defer func() {
		if err = database.Close(); err != nil {
			zap.L().Error("main",
				zap.String("info", "Could not close database!"),
				zap.Error(err))
		}
	}()

	if opFlags.RunWeb {
		go web.StartWeb(opFlags.Addr, opFlags.Credentials, database)
	}

	var persistentStorage storage.PersistentStorageServer
	if opFlags.RunStorage {
		persistentStorage = storage.StartPersistentStorage(opFlags.QueueServiceUrl, opFlags.QueueServicePersistsDatabaseURL)
		if persistentStorage == nil {
			return
		}
		_ = persistentStorage.HandlerTorrent()
	}
	defer func() {
		if persistentStorage != nil {

			if err = persistentStorage.Close(); err != nil {
				zap.L().Error("main",
					zap.String("info", "Could not close Queue!"),
					zap.Error(err))
			}
		}
	}()

	if !opFlags.RunDaemon {
		<-interruptChan
		return
	}

	trawlingManager := dht.NewManager(opFlags.IndexerAddrs, opFlags.IndexerMaxNeighbors, opFlags.BootstrappingNodes, opFlags.BootstrapNodesSelfPort, opFlags.FilterNodesCIDRs)
	metadataSink := metadata.NewSink(5*time.Second, opFlags.LeechMaxN, opFlags.FilterNodesCIDRs)

	// The Event Loop
	for stopped := false; !stopped; {
		select {
		case result := <-trawlingManager.Output():
			infoHash := result.InfoHash()

			exists, err := database.DoesTorrentExist(infoHash[:])
			if err != nil {
				go stats.GetInstance().IncDBError(false)
			} else if !exists {
				metadataSink.Sink(result)
			}

		case md := <-metadataSink.Drain():
			if err := database.AddNewTorrent(md.InfoHash, md.Name, md.Files); err != nil {
				go stats.GetInstance().IncDBError(true)
			}

		case <-interruptChan:
			trawlingManager.Terminate()
			stopped = true
		}
	}
}

func parseFlags() error {
	var cmdF struct {
		RunWithConfigFile bool   `long:"with-config-file" description:"Run using yaml configuration file."`
		ConfigFilePath    string `long:"config-file-path" description:"Configuration file path. If not filled in, it will default to config.yml in the same directory of this program."`

		QueueServiceUrl                 string `long:"queue" description:"The address of the queue service link." default:"amqp://rabbitmq:rabbitmq@localhost:5672/magnetico" mapstructure:"queueServiceURL"`
		QueueServicePersistsDatabaseURL string `long:"queue-persists" description:"URL of the persistent database for queue data." default:"postgres://magnetico:magnetico@localhost:5432/magnetico?sslmode=disable" mapstructure:"queueServicePersistsDatabaseURL"`
		DatabaseURL                     string `long:"database" description:"URL of the Persistent database." default:"postgres://magnetico:magnetico@localhost:5432/magnetico?sslmode=disable" mapstructure:"databaseURL"`

		IndexerAddrs        []string `long:"indexer-addr" description:"Address(es) to be used by indexing DHT nodes." default:"0.0.0.0:0" mapstructure:"indexerAddrs"`
		IndexerMaxNeighbors uint     `long:"indexer-max-neighbors" description:"Maximum number of neighbors of an indexer." default:"5000" mapstructure:"indexerMaxNeighbors"`

		LeechMaxN uint `long:"leech-max-n" description:"Maximum number of leeches." default:"1000" mapstructure:"leechMaxN"`
		MaxRPS    uint `long:"max-rps" description:"Maximum requests per second." default:"500" mapstructure:"maxRPS"`

		BootstrappingNodes     []string `long:"bootstrap-node" description:"Host(s) to be used for bootstrapping." default:"dht.tgragnato.it" mapstructure:"bootstrappingNodes"`
		BootstrapNodesSelfPort bool     `long:"is-bootstrap-node-self-port" description:"Customize the port of the boot host(s)." mapstructure:"bootstrapNodesSelfPort"`
		FilterNodesCIDRs       []string `long:"filter-nodes-cidrs" description:"List of CIDRs on which Magnetico can operate. Empty is open mode." default:"" mapstructure:"filterNodesCIDRs"`

		Addr string `short:"a" long:"addr"        description:"Address (host:port) to serve on" default:"[::1]:8080" mapstructure:"addr"`
		Cred string `short:"c" long:"credentials" description:"Path to the credentials file" default:"" mapstructure:"cred"`

		RunDaemon  bool `short:"d" long:"daemon" description:"Run the crawler without the web interface." mapstructure:"runDaemon"`
		RunWeb     bool `short:"w" long:"web"    description:"Run the web interface without the crawler." mapstructure:"runWeb"`
		RunStorage bool `short:"s" long:"storage" description:"Fetch and persist storage from the queue.You need to set both queue and queue-persists parameters" mapstructure:"runStorage"`

		IsDevEnv bool   `long:"is-dev-env" description:"Enable developer mode." mapstructure:"isDevEnv"`
		LogLevel string `long:"log-level" description:"Set the log level.Support: debug,info,warn,error,fatal,panic,dpanic" default:"warn" mapstructure:"logLevel"`
	}

	if _, err := flags.Parse(&cmdF); err != nil {
		return err
	}

	if cmdF.RunWithConfigFile {

		if cmdF.ConfigFilePath == "" {
			execPath, err := os.Executable()
			if err != nil {
				return err
			}
			execPath, err = filepath.Abs(execPath)
			if err != nil {
				return err
			}
			cmdF.ConfigFilePath = path.Join(filepath.Dir(execPath), "config.yml")
		}
		configFilePath := cmdF.ConfigFilePath
		configFileNameWithSuffix := path.Base(configFilePath)
		configFileType := path.Ext(configFileNameWithSuffix)
		configFileNameOnly := strings.TrimSuffix(configFileNameWithSuffix, configFileType)
		vip := viper.New()
		vip.SetConfigName(configFileNameOnly)
		vip.SetConfigType(configFileType[1:])
		vip.AddConfigPath(filepath.Dir(configFilePath))
		err := vip.ReadInConfig()
		if err != nil {
			return err
		}
		err = vip.Unmarshal(&cmdF)
		if err != nil {
			return err
		}
		_ = vip.MergeConfigMap(vip.AllSettings())

	}

	if cmdF.RunDaemon && !cmdF.RunWeb {
		opFlags.RunDaemon = true
		opFlags.RunWeb = false
	} else if !cmdF.RunDaemon && cmdF.RunWeb {
		opFlags.RunDaemon = false
		opFlags.RunWeb = true
	} else {
		opFlags.RunDaemon = false
		opFlags.RunWeb = false
	}

	if cmdF.RunStorage {
		opFlags.RunStorage = true
		opFlags.QueueServiceUrl = cmdF.QueueServiceUrl
		opFlags.QueueServicePersistsDatabaseURL = cmdF.QueueServicePersistsDatabaseURL
	}

	opFlags.DatabaseURL = cmdF.DatabaseURL

	if opFlags.RunWeb {
		opFlags.Addr = cmdF.Addr
		opFlags.CredentialsPath = cmdF.Cred

		opFlags.Credentials = make(map[string][]byte)
		if err := loadCred(opFlags.CredentialsPath); err == nil {
			web.UpdateCredentials(opFlags.Credentials)
		}
	}

	if opFlags.RunDaemon {
		if err := checkAddrs(cmdF.IndexerAddrs); err != nil {
			zap.L().Fatal("main",
				zap.String("info", "Of argument (list) `trawler-ml-addr`"),
				zap.Error(err))
		} else {
			opFlags.IndexerAddrs = cmdF.IndexerAddrs
		}

		opFlags.IndexerMaxNeighbors = cmdF.IndexerMaxNeighbors

		opFlags.LeechMaxN = int(cmdF.LeechMaxN)
		if opFlags.LeechMaxN > 1000 {
			zap.L().Warn("main",
				zap.String("info", "Beware that on many systems max # of file descriptors per process is limited to 1024. "+
					"Setting maximum number of leeches greater than 1k might cause \"too many open files\" errors!"))
		}

		mainline.DefaultThrottleRate = int(cmdF.MaxRPS)
		opFlags.BootstrappingNodes = cmdF.BootstrappingNodes

		opFlags.BootstrapNodesSelfPort = cmdF.BootstrapNodesSelfPort

		opFlags.FilterNodesCIDRs = []net.IPNet{}
		for _, cidr := range cmdF.FilterNodesCIDRs {
			if cidr == "" {
				continue
			}
			if _, ipnet, err := net.ParseCIDR(cidr); err == nil {
				opFlags.FilterNodesCIDRs = append(opFlags.FilterNodesCIDRs, *ipnet)
			} else {
				zap.L().Fatal("main",
					zap.String("info", "Error while parsing CIDR "+cidr),
					zap.Error(err))
			}
		}
		if len(opFlags.FilterNodesCIDRs) != 0 && reflect.DeepEqual(cmdF.BootstrappingNodes, []string{"dht.tgragnato.it"}) {
			zap.L().Fatal("main",
				zap.String("info", "You should specify your own internal bootstrapping nodes in filter mode."))
		}
	}

	if cmdF.IsDevEnv {
		opFlags.IsDevEnv = true
	}

	switch cmdF.LogLevel {
	case "debug":
		opFlags.LogLevel = zap.DebugLevel
	case "info":
		opFlags.LogLevel = zap.InfoLevel
	case "warn":
		opFlags.LogLevel = zap.WarnLevel
	case "error":
		opFlags.LogLevel = zap.ErrorLevel
	case "fatal":
		opFlags.LogLevel = zap.FatalLevel
	case "panic":
		opFlags.LogLevel = zap.PanicLevel
	case "dpanic":
		opFlags.LogLevel = zap.DPanicLevel
	default:
		opFlags.LogLevel = zap.WarnLevel
	}

	return nil
}

func checkAddrs(addrs []string) error {
	for _, addr := range addrs {
		// We are using ResolveUDPAddr but it works equally well for checking TCPAddr(esses) as
		// well.
		_, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadCred(cred string) error {
	if cred == "" {
		return nil
	}

	file, err := os.Open(cred)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	for lineno := 1; true; lineno++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.New("Error while reading line " + strconv.Itoa(lineno) + " " + err.Error())
		}

		line = line[:len(line)-1] // strip '\n'

		/* The following regex checks if the line satisfies the following conditions:
		 *
		 * <USERNAME>:<BCRYPT HASH>
		 *
		 * where
		 *     <USERNAME> must start with a small-case a-z character, might contain non-consecutive
		 *   underscores in-between, and consists of small-case a-z characters and digits 0-9.
		 *
		 *     <BCRYPT HASH> is the output of the well-known bcrypt function.
		 */
		re := regexp.MustCompile(`^[a-z](?:_?[a-z0-9])*:\$2[aby]?\$\d{1,2}\$[./A-Za-z0-9]{53}$`)
		if !re.Match(line) {
			return fmt.Errorf("on line %d: format should be: <USERNAME>:<BCRYPT HASH>, instead got: %s", lineno, line)
		}

		tokens := bytes.Split(line, []byte(":"))
		opFlags.Credentials[string(tokens[0])] = tokens[1]
	}

	return nil
}
