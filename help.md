# Stream In — source réseau SRT/RTMP

Ingère un flux réseau (contribution, OBS, encodeur distant…) et le publie sur le bus MXL
comme n'importe quelle source : câblable vers un multiview, un mélangeur, une sortie 2110…

## Modes d'ingest

- **SRT — écoute** : l'instance écoute sur son port ; la source distante pousse en mode
  *caller* vers l'URL affichée dans l'UI (`srt://<ip>:<port>?mode=caller`).
- **SRT — appel** : l'instance se connecte à une URL SRT distante (la source est en *listener*).
- **RTMP — serveur** : l'instance écoute en `rtmp://` ; une source type OBS pousse vers
  `rtmp://<ip>:<port>/<application>/<clé>`. Un seul publieur à la fois.
- **RTMP — pull** : l'instance tire un flux `rtmp://` existant.

L'IP affichée est celle du **conteneur** (macvlan) : elle est directement joignable sur le
LAN, sans redirection de port. Le port d'écoute est libre — vérifier qu'il ne collisionne
pas avec une autre instance sur le même nœud.

**Passphrase / Stream ID** (modes SRT) : la passphrase chiffre le transport (AES, à
renseigner identique des deux côtés) ; le Stream ID sert au routage/authentification côté
certains encodeurs ou plateformes (Haivision, Wowza…) qui l'exigent pour accepter la
connexion. Les deux sont facultatifs et s'appliquent aux deux modes SRT (écoute et appel).

## Point d'entrée

L'adresse affichée dans le panneau **est** l'adresse réglable : le port (modes écoute), les
segments application et clé (RTMP serveur) ou l'URL distante (modes appel et pull) s'éditent
directement là où ils sont lus. Appliquer relance la connexion sans redéployer, la sortie MXL
n'est pas coupée. Les huit dernières adresses utilisées restent accessibles en un clic.

L'**interrupteur Ingest** coupe ffmpeg sans arrêter le conteneur : la sortie continue d'émettre
la mire à la cadence configurée, les consommateurs câblés ne voient aucune rupture.

## Journal de connexion

Chaque transition d'état, redémarrage de ffmpeg, erreur, changement de source et bascule
marche/arrêt est horodaté dans un journal persistant, qui survit au redémarrage du script.
Consultable par plage date/heure et exportable en CSV : c'est la pièce justificative quand il
faut établir d'où venait une coupure. Les horodatages viennent du conteneur, dont l'horloge est
disciplinée par PTP, donc comparables avec le reste de la chaîne. Vider le journal est possible,
demande confirmation, et l'effacement est lui-même consigné.

## Format de sortie (conform)

Le format de sortie se choisit au déploiement, dans le sélecteur de format de la palette,
alimenté par Réglages → Formats vidéo. Le panneau affiche côte à côte le format **réel** de la
source (avant conform) et le format de sortie.

Le flux entrant est **toujours reconverti** au format configuré (résolution, cadence, chroma,
profondeur, balayage) : scale bicubique, conversion de cadence, désentrelacement automatique
si l'entrée est entrelacée. La sortie MXL est donc un signal broadcast propre et stable quel
que soit ce qu'envoie la source. Pour une sortie entrelacée (ex. 1080i50) : cadence = cadence
**trame** (25) et balayage = entrelacé.

## Genlock

**Genlock (grille PTP)** (activé par défaut) : l'image de sortie est écrite sur un index
calé sur la grille PTP rationnelle — une trame par tick, comme un producteur normal du
pipeline, alignée avec les autres sources pour un mixage/multiview cohérent. Désactivé, la
sortie suit le rythme d'arrivée de la source (index libre) : à réserver aux cas où l'aval ne
dépend d'aucune synchro de grille (rare — ne pas désactiver sans raison précise).

## États

- **En attente de flux** : aucune image reçue — l'instance émet la mire (ou du noir, réglage
  « Image sans flux »).
- **Connecté** : images fraîches, sortie conformée en direct.
- **Gelé** : plus d'image depuis le seuil configuré — la dernière image est répétée (le signal
  MXL ne s'interrompt jamais). Reprise automatique dès que le flux revient ; en modes
  appel/pull, une connexion zombie est tuée et retentée automatiquement.

## Audio

L'audio du flux est décodé vers un shm 8 canaux 48 kHz (câblable page Câbles). Sans piste
audio dans le flux, l'instance bascule automatiquement en vidéo seule (silence sur le shm).

## Synchro A/V

> ⚠ **Le verrou est DÉSACTIVÉ par défaut** (`av_lock`). Mesuré le 2026-08-23 sur une
> liaison SRT réelle, il fait pire que pas de verrou : écart A/V de +275 ms au lieu de
> +38 ms, et âge audio moins stable. Deux défauts identifiés (erreur d'origine d'environ
> 270 ms, verrou qui se déplace trop souvent) — voir `TODO.md`. Ce qui suit décrit ce
> qu'il fait quand on l'active, pour y travailler.

L'audio n'est pas débité au fil de l'eau : il est posé **à la case de grille de la trame qui lui
correspond**. Les deux filtres ffmpeg de la chaîne portent la même référence temporelle — la
trame de sortie n° *n* est au pts source *n·D/N*, l'échantillon n° *s* au pts *s/48000* — donc
trame *n* ↔ échantillon *n·48000·D/N*. Le plugin en déduit un décalage entier unique et écrit
l'audio dessus. Sans ce verrou (`av_lock` décoché), chaque essence est cadencée séparément et la
latence audio n'est rien d'autre que l'occupation de son tampon : elle erre, l'écart A/V ne tient
pas — mesuré ±100 ms sur une liaison SRT réelle.

**Retenue vidéo** (`av_margin_frames`, 1 trame par défaut). Verrouiller l'audio sur l'instant de
présentation vidéo lui retire sa marge d'arrivée : le tampon ne la porte plus, et un bloc en
retard devient du silence. La vidéo est donc présentée une trame plus tard — **les deux essences
décalées d'autant, le lip-sync ne bouge pas** — pour rendre cette marge au chemin audio. C'est
`audio_headroom_ms` (ci-dessous) qui doit décider de sa valeur, pas une estimation.

L'état de connexion, lui, continue de se juger sur l'arrivée courante : la retenue décale
l'image, elle ne vieillit pas le diagnostic et ne provoque pas de faux gel.

## Diagnostic audio

`:8080` expose la santé du chemin audio — en régime établi, les compteurs doivent rester figés :

- `av_locked` : le décalage A/V est **établi** (une trame présentée depuis le démarrage de
  ffmpeg). À distinguer de `av_lock`, qui ne dit que l'état de la case à cocher : un verrou
  demandé mais jamais accroché est précisément le cas qu'il faut voir.
- `audio_headroom_ms` : **marge d'arrivée minimale sur la seconde écoulée** — de combien
  l'audio devance sa case. Négatif = il arrive après, et ce qui manque part en silence. C'est la
  mesure qui gouverne le réglage de la retenue vidéo.
- `av_relocks` : le verrou SUIT une dérive lente (médiane de fenêtre, confirmée sur deux
  fenêtres). Par pas d'une trame.
- `av_reanchors` : le verrou a été RELÂCHÉ parce que plus rien n'était servi — un décrochage,
  pas une dérive. Attendu après une reconnexion SRT (ffmpeg rattrape à 3× le temps réel et la
  relation « trame ↔ case » ne tient pas pendant la rafale) ; anormal en régime établi.
- `av_skew_ms` : écart entre la ligne de temps DEMANDÉE et celle qui ARRIVE. Doit être à 0 en
  régime établi. Une valeur franche et stable dit que le verrou vise à côté.
- `av_fallback` : non nul = le verrou a renoncé et l'audio est débité au fil de l'eau
  (comportement d'avant la 0.9.0). Retenté automatiquement. Le voir en permanence sur une source
  donnée est un signalement à remonter, pas un réglage à faire.
- `audio_late_samples` : échantillons arrivés après leur case, donc jetés.
- `audio_latence_ms` : **la latence audio réelle** — le « bas de l'eau », ce qui reste en
  permanence dans le tampon sur une fenêtre de 2 s. C'est cette valeur que le drainage ramène
  vers 30 ms, et la seule qui ait le sens d'une latence.
- `audio_buf_ms` : occupation **de pic**, relevée juste après un ajout. Elle mesure la taille des
  rafales d'écriture de ffmpeg (300–400 ms couramment), **pas** la latence. Ne pas dimensionner
  quoi que ce soit dessus : c'est ce qui avait mis le plafond du tampon à 200 ms, sous la taille
  des rafales, d'où 40 % de l'audio jeté à l'entrée puis autant de silence.
- `audio_drop_plafond` / `audio_drop_drainage` / `audio_drop_reancrage` / `audio_drop_generation` :
  QUI a jeté. Le plafond qui monte en régime établi = capacité insuffisante ou consommation en
  retard ; le drainage = de la latence installée qu'on rend (normal, rare) ; le ré-ancrage = la
  grille a décroché ; la génération = un reliquat d'avant une reconnexion.
- `audio_starve_blocks` : blocs de silence insérés faute d'audio frais (s'incrémente pendant
  l'attente d'une source, c'est normal).
- `audio_drop_samples` : échantillons jetés (drainage sans verrou, reliquats de génération).
- `audio_reanchors` : ré-ancrages sur la grille TAI (dérive > 250 ms de l'horloge source).
- `audio_in_sps` : échantillons/s livrés par ffmpeg (≈ 48000).

Le flux audio est écrit sur le bus par paquets dont la taille vient du **réglage du cœur**
(Réglages → MXL, `mxl_audio_grain_us`, 1 ms par défaut) : le plugin la lit par
`bobimxl.audio_grain()` et le flux l'annonce, il ne la décide pas. Ce fichier affirmait « ne pas
augmenter cette granularité » quand le 1 ms était une convention recopiée dans chaque plugin ; elle
est désormais déclarée en un seul endroit, et un plugin écrit avant le réglage retombe sur 1 ms.

## Mode tranche (latence réduite)

Paramètre avancé `slice_mode` (désactivé par défaut, comportement inchangé) : la sortie
vidéo — déjà décodée/conformée par ffmpeg — est publiée bande par bande sur le bus MXL au
lieu d'attendre l'image complète, pour retirer une image de latence à l'aval (multiview,
mixer, streamer en mode tranche). `slice_lines` fixe la hauteur des bandes et doit diviser
la hauteur de sortie (36 pour 1080p) ; sinon repli automatique en image entière. Une source
entrelacée retombe aussi en image entière. N'affecte ni l'ingest ni l'audio.

## Contrôle

- `GET /state` : état complet (connexion, fps entrée/sortie, âge de la dernière image,
  redémarrages ffmpeg, derniers messages d'erreur).
- `POST /reconnect` : force la coupure et la relance de la connexion (re-tente aussi l'audio).
