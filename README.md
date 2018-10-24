![alt text](docs/images/priam.png "Priam Logo")

## Documentation Updates

To update the documentation, follow below steps

Documentation helpers commands are in tox, so ensure you have tox installed on your machine

```
pip install tox
```
Change the documentation inside `docs/` folder. 

*To build:*

```
tox -e build
```

*To test the changes locally:*
```
tox -e test
```

*To deploy the changes to `gh-pages` branch:*
```
tox -e publish
```

## Changelog
See [CHANGELOG.md](CHANGELOG.md)

<!-- 
References
-->
[release]:https://github.com/Netflix/Priam/releases/latest "Latest Release (external link) ➶"
[wiki]:https://github.com/Netflix/Priam/wiki
[repo]:https://github.com/Netflix/Priam
[img-travis-ci]:https://travis-ci.org/Netflix/Priam.svg?branch=3.x
[travis-ci]:https://travis-ci.org/Netflix/Priam

