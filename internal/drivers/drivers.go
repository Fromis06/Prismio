package drivers

import (
	_ "my-cdc/internal/capture/postgres"
	_ "my-cdc/internal/sinks/postgres"
	_ "my-cdc/internal/sinks/sqlserver"
)
