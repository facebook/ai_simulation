# AI Simulation

[![linux](https://github.com/facebook/ai_simulation/actions/workflows/getdeps_linux.yml/badge.svg)](https://github.com/facebook/ai_simulation/actions/workflows/getdeps_linux.yml)

This repository holds tools Meta uses to simulate the backend networks that
carry AI training and inference traffic. Training a large model is bounded as
much by the fabric between the accelerators as by the accelerators themselves,
and the questions that matter — how a topology behaves under a given collective,
where a buffer overruns, what a congestion-control change actually buys — are
easier to answer in simulation than on hardware that has not been built yet.
We are sharing the tools we use to answer them.

## Projects

- **[packsim](packsim/)** — a packet-level, discrete-event simulator for AI
  training fabrics. Models endpoints, NICs, RoCEv2 transport, switches and
  links, and reports how long a collective takes and where the time went.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first — how changes reach this
repository is unusual. It is generated from a directory inside Meta's internal
monorepo, so a pull request here is never merged: if we take the change, a Meta
engineer applies it internally and it arrives on the next export, with you
credited. The marker comments you will see in the sources are load-bearing and
should be left alone; CONTRIBUTING.md explains them.

Participants are expected to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Citing

Each project in this repository has its own citation.

### PackSim

If you use PackSim in your research, don't forget to cite us!

```bibtex
@misc{noormohammadpour2026packsim,
  author       = {Noormohammadpour, Max and Du, Yong and Zhang, Yuhui and
                  Costello, Kevin and Spring, Neil and Lou, Mike and
                  Wang, Zhaodong and Cai, Yan and Larkin, Pavel and
                  Steinbrecher, Greg and Ahuja, Satyajeet Singh and
                  Nayak, Mahesh and Patel, Prasoon and Zhang, Ying},
  title        = {{PackSim}: A Packet-Level, Discrete-Event Simulator for
                  {AI} Training Fabrics},
  year         = {2026},
  howpublished = {\url{https://github.com/meta/ai_simulation/tree/main/packsim}}
}
```

## License

PackSim is licensed under the BSD 3-Clause License, as found in the
[LICENSE](LICENSE) file. `build/fbcode_builder/`, Meta's shared build tooling,
has its own licence terms: see its [LICENSE](build/fbcode_builder/LICENSE) and
the headers of its files.
