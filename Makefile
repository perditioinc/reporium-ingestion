# Root passthrough to the local $0/OSS development substrate (see local/).
# These targets only ever touch local containers — never production or cloud.

.PHONY: local-up local-down local-seed local-smoke local-logs local-ps local-help

local-help:
	$(MAKE) -C local help

local-up:
	$(MAKE) -C local up

local-down:
	$(MAKE) -C local down

local-seed:
	$(MAKE) -C local seed

local-smoke:
	$(MAKE) -C local smoke

local-logs:
	$(MAKE) -C local logs

local-ps:
	$(MAKE) -C local ps
