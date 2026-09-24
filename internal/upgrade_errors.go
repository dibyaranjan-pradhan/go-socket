// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import "errors"

// errTransportUpgraded tells sessionTransport to retry Read on the post-upgrade transport.
var errTransportUpgraded = errors.New("gosocket: transport upgraded")
