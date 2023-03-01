VERSION := $(shell grep version pyproject.toml | sed -r 's/^version\s*=\s*"(\S+?)"/\1/g')
.PHONY: pylama
pylama:
	pylama --ignore=E501,E301,E265,E266 polymer/*.py | less -XR
.PHONY: clean
clean:
	find ./* -name '*.pyc' -exec rm {} \;
	find ./* -name '*.so' -exec rm {} \;
	find ./* -name '*.coverage' -exec rm {} \;
	@# A minus sign prefixing the line means it ignores the return value
	-find ./* -path '*__pycache__' -exec rm -rf {} \;
	-rm -rf .pytest_cache/
	-rm -rf .eggs/
	-rm -rf .cache/
	-rm -rf build/ dist/ polymer.egg-info/ setuptools*

.PHONY: repo-push
repo-push:
	git remote remove origin
	git remote add origin "git@github.com:mpenning/polymer"
	git push git@github.com:mpenning/polymer.git
	git push origin +master

.PHONY: repo-push-tag
repo-push-tag:
	git remote remove origin
	git remote add origin "git@github.com:mpenning/polymer"
	git tag -a ${VERSION} -m "Tag with ${VERSION}"
	git push git@github.com:mpenning/polymer.git
	git push origin +master
	git push --tags origin ${VERSION}

.PHONY: repo-push-force
repo-push-force:
	git remote remove origin
	git remote add origin "git@github.com:mpenning/polymer"
	git push git@github.com:mpenning/polymer.git
	git push --force-with-lease origin +master

.PHONY: repo-push-tag-force
repo-push-tag-force:
	git remote remove origin
	git remote add origin "git@github.com:mpenning/polymer"
	git tag -a ${VERSION} -m "Tag with ${VERSION}"
	git push git@github.com:mpenning/polymer.git
	git push --force-with-lease origin +master
	git push --force-with-lease --tags origin ${VERSION}

.PHONY: pypi
pypi:
	make clean
	poetry build
	python -m twine upload dist/*

