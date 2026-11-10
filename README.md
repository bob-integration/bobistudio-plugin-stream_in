# Stream In — source réseau SRT/RTMP de Bobi.Studio

*[English version](README.en.md)*

Fait entrer un flux SRT ou RTMP sur le bus MXL comme n'importe quelle autre source. Sous-module de
[bobi.studio](https://github.com/bob-integration/bobistudio), monté sur `plugins/stream_in/`.

## Ce que ça fait

Reçoit un flux réseau et le publie sur le bus : une sortie vidéo (`<hôte>`) et une sortie audio
8 canaux (`<hôte>_audio`).

Quatre modes. En **écoute** (SRT) ou en **serveur** (RTMP), la source pousse vers l'adresse
affichée par le plugin. En **appel** (SRT) ou en **pull** (RTMP), le plugin va chercher le flux.
L'adresse affichée est celle du conteneur, joignable directement, sans redirection de port.

En SRT, le plugin gère la passphrase et le Stream ID.

## Point d'entrée

Port, application et clé RTMP, URL distante : tout s'édite dans le panneau. Appliquer relance la
connexion sans redéployer et sans couper la sortie. Les huit dernières adresses restent
accessibles en un clic.

L'interrupteur d'ingest coupe la réception sans arrêter le conteneur : la sortie continue
d'émettre la mire à la cadence configurée.

## Journal de connexion

Transitions d'état, redémarrages, erreurs, changements de source et bascules marche/arrêt sont
horodatés dans un journal persistant, consultable par plage horaire et exportable en CSV. Les
horodatages viennent de l'horloge du conteneur, disciplinée par PTP.

## Format de sortie

Le flux entrant est toujours reconverti au format configuré — résolution, cadence, chroma,
profondeur, balayage —, avec désentrelacement automatique d'une entrée entrelacée. Le panneau
affiche côte à côte le format réel de la source et le format de sortie.

Le genlock, actif par défaut, cale la sortie sur la grille PTP. Désactivé, la sortie suit le rythme
d'arrivée du flux.

## États

**En attente** : aucune image reçue, le plugin émet la mire ou du noir. **Connecté** : images
fraîches. **Gelé** : plus d'image depuis le seuil configuré, la dernière est répétée. La reprise
est automatique ; en appel ou en pull, une connexion morte est coupée et retentée.

Sans piste audio dans le flux, le plugin écrit du silence sur sa sortie audio.

## Synchro A/V

Un verrou A/V pose l'audio sur la case de grille de la trame qui lui correspond. Il est
**désactivé par défaut** : mesuré le 2026-08-23 sur une liaison SRT réelle, il donne +275 ms
d'écart A/V contre +38 ms sans lui. Deux défauts sont identifiés et en cours de correction.

## Diagnostic audio

`:8080` publie la santé du chemin audio. `audio_latence_ms` est la latence audio ;
`audio_buf_ms` est l'occupation de pic, qui mesure la taille des rafales d'écriture (300 à 400 ms
couramment) et non la latence — à ne pas utiliser pour dimensionner. Les pertes sont comptées par
cause (`audio_drop_plafond`, `_drainage`, `_reancrage`, `_generation`). En régime établi, ces
compteurs restent figés.

## Mode tranche

La sortie vidéo peut être publiée bande par bande, pour retirer une trame de latence à l'aval.
Une source entrelacée, ou une hauteur de bande qui ne divise pas l'image, retombe en image entière.
Désactivé par défaut.

## Licence

GNU GPL v3 ou ultérieure — voir `LICENSE`.
